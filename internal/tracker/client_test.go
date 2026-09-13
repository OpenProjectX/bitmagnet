package tracker

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func catalogue(t *testing.T) []byte {
	t.Helper()

	b, e := bencode.Marshal(
		map[string]any{
			"files": map[string]any{
				strings.Repeat("a", 20): map[string]int{
					"complete":   2,
					"incomplete": 3,
					"downloaded": 4,
				},
			},
		},
	)
	require.NoError(t, e)

	return b
}

func TestScrapeBoundaries(t *testing.T) {
	t.Parallel()

	b := catalogue(t)
	for _, test := range []struct {
		name  string
		data  []byte
		limit int
		want  int
		bad   bool
	}{
		{"valid", b, 1, 1, false},
		{"empty", []byte("d5:filesdee"), 1, 0, false},
		{"truncated", b[:len(b)-1], 1, 1, true},
		{"trailing", append(append([]byte{}, b...), 'x'), 1, 1, true},
		{"cap", b, 0, 0, true},
		{"html", []byte("<html>ok</html>"), 1, 0, true},
		{"failure", []byte("d14:failure reason8:disablede"), 1, 0, true},
		{"huge_string", []byte("d99999999:"), 1, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := 0
			n, e := ParseScrape(
				bytes.NewReader(test.data),
				test.limit,
				func(entry Entry) error { got++; require.Equal(t, int64(2), entry.Seeders); return nil },
			)

			require.Equal(t, test.want, got)
			require.Equal(t, got, n)
			require.Equal(t, test.bad, e != nil)
		})
	}

	_, e := ParseScrape(
		bytes.NewReader(b),
		1,
		func(Entry) error { return errors.New("backpressure") },
	)
	require.ErrorContains(t, e, "backpressure")

	nested := "d1:x" + strings.Repeat("l", 20) + strings.Repeat("e", 20) + "e"
	_, e = ParseScrape(strings.NewReader(nested), 1, func(Entry) error { return nil })
	require.ErrorContains(t, e, "nesting")
}

func TestHTTPGzipAndLimits(t *testing.T) {
	t.Parallel()
	raw := catalogue(t)

	var compressed bytes.Buffer
	g := gzip.NewWriter(&compressed)
	_, _ = g.Write(raw)
	require.NoError(t, g.Close())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/scrape", r.URL.Path)
		assert.Empty(t, r.URL.Query().Get("info_hash"))
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(compressed.Bytes())
	}))
	defer server.Close()

	client := &Client{http: server.Client()}

	for _, limit := range []int64{4096, 5} {
		var wireBytes, decodedBytes int64

		n, e := client.Scrape(
			context.Background(),
			server.URL+"/scrape",
			Limits{
				WireBytes: 4096, DecodedBytes: limit, Hashes: 10, Timeout: time.Second,
				OnRead: func(wire, decoded int64) { wireBytes, decodedBytes = wire, decoded },
			},
			func(Entry) error { return nil },
		)
		if limit == 5 {
			require.Error(t, e)
		} else {
			require.NoError(t, e)
			require.Equal(t, 1, n)
			require.EqualValues(t, compressed.Len(), wireBytes)
			require.EqualValues(t, len(raw), decodedBytes)
		}
	}
}

func TestMetadataOnlyAnnounceAndBinaryHash(t *testing.T) {
	t.Parallel()

	hash := protocol.ID{0, 32, 43, 255, 38}
	peer := protocol.RandomPeerID()
	events := []string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assert.Equal(t, string(hash[:]), q.Get("info_hash"))
		assert.Equal(t, string(peer[:]), q.Get("peer_id"))
		assert.Equal(t, "-1", q.Get("left"))
		assert.Equal(t, "0", q.Get("uploaded"))
		assert.Equal(t, "0", q.Get("downloaded"))
		assert.Equal(t, "12345", q.Get("port"))
		events = append(events, q.Get("event"))
		data, _ := bencode.Marshal(
			map[string]any{
				"interval": 1800,
				"peers":    string([]byte{8, 8, 8, 8, 0x1a, 0xe1, 127, 0, 0, 1, 0, 80}),
			},
		)
		_, _ = w.Write(data)
	}))
	defer server.Close()

	c := &Client{http: server.Client()}
	for _, event := range []string{"started", "stopped"} {
		res, e := c.Announce(context.Background(), server.URL+"/announce", hash, peer, 12345, event)
		require.NoError(t, e)
		require.Equal(t, 30*time.Minute, res.Interval)
		require.Equal(t, []netip.AddrPort{netip.MustParseAddrPort("8.8.8.8:6881")}, res.Peers)
	}

	require.Equal(t, []string{"started", "stopped"}, events)
}

func TestRetryAfterAndAddressPolicy(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	c := &Client{http: server.Client()}
	_, e := c.Scrape(
		context.Background(),
		server.URL,
		Limits{Timeout: time.Second},
		func(Entry) error { return nil },
	)

	var he HTTPError

	require.ErrorAs(t, e, &he)
	require.Equal(t, time.Hour, he.RetryAfter)

	publicClient := NewClient()
	defer publicClient.Close()
	_, e = publicClient.Scrape(
		context.Background(),
		server.URL,
		Limits{Timeout: time.Second},
		func(Entry) error { return nil },
	)
	require.ErrorContains(t, e, "non-public")

	for _, ip := range []string{
		"127.0.0.1", "::1", "10.0.0.1", "169.254.169.254", "100.64.0.1", "::ffff:192.168.1.1", "224.1.1.1",
	} {
		require.False(t, PublicAddress(netip.MustParseAddr(ip)), ip)
	}

	require.True(t, PublicAddress(netip.MustParseAddr("8.8.8.8")))

	u, e := ScrapeURL("https://example.org/announce.php?pass=abc")
	require.NoError(t, e)
	require.Equal(t, "https://example.org/scrape.php?pass=abc", u)

	_, e = ScrapeURL("udp://example.org:80/announce")
	require.Error(t, e)
}

func TestMalformedStats(t *testing.T) {
	t.Parallel()

	for _, value := range []any{
		map[string]int{"complete": -1, "incomplete": 0, "downloaded": 0}, map[string]int{"complete": 1}, "wrong",
	} {
		b, e := bencode.Marshal(
			map[string]any{"files": map[string]any{strings.Repeat("a", 20): value}},
		)
		require.NoError(t, e)
		_, e = ParseScrape(
			bytes.NewReader(b),
			10,
			func(Entry) error { return fmt.Errorf("should never emit") },
		)
		require.ErrorContains(t, e, "invalid scrape")
	}
}
