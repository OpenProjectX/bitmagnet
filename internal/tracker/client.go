// Package tracker implements bounded HTTP(S) full scrapes and metadata-only announces.
package tracker

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

type Entry struct {
	Hash                          protocol.ID
	Seeders, Leechers, Downloaded int64
}
type Limits struct {
	WireBytes, DecodedBytes int64
	Hashes                  int
	Timeout                 time.Duration
	// OnRead receives consumed response body bytes, excluding HTTP headers.
	OnRead func(wire, decoded int64)
}
type (
	Client    struct{ http *http.Client }
	HTTPError struct {
		Status     int
		RetryAfter time.Duration
	}
)

func (e HTTPError) Error() string { return fmt.Sprintf("tracker HTTP status %d", e.Status) }

func ValidateURL(raw string) (*url.URL, error) {
	u, e := url.Parse(raw)
	if e != nil {
		return nil, e
	}

	if (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil ||
		u.Fragment != "" {
		return nil, errors.New("expected public HTTP(S) URL without credentials or fragment")
	}

	return u, nil
}

func ScrapeURL(raw string) (string, error) {
	u, e := ValidateURL(raw)
	if e != nil {
		return "", e
	}

	if !strings.Contains(u.Path, "announce") {
		return "", errors.New("announce path needs explicit scrape URL")
	}

	u.Path = strings.Replace(u.Path, "announce", "scrape", 1)
	u.RawPath = ""

	return u.String(), nil
}

var excludedNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"), netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix(
		"2001::/23",
	), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
}

func PublicAddress(a netip.Addr) bool {
	a = a.Unmap()
	if !a.IsValid() || !a.IsGlobalUnicast() || a.IsPrivate() || a.IsLoopback() ||
		a.IsLinkLocalUnicast() {
		return false
	}

	for _, p := range excludedNetworks {
		if p.Contains(a) {
			return false
		}
	}

	return true
}

func NewClient() *Client {
	// Direct egress: do not inherit an unrelated TMDB proxy that can bypass address validation.
	t := &http.Transport{
		DisableCompression: true, MaxIdleConnsPerHost: 2, ResponseHeaderTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, e := net.SplitHostPort(address)
			if e != nil {
				return nil, e
			}
			ips, e := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			if e != nil {
				return nil, e
			}
			var last error
			for _, ip := range ips {
				if !PublicAddress(ip) {
					return nil, errors.New("non-public tracker address")
				}
			}
			for _, ip := range ips {
				c, e := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(
					ctx,
					network,
					net.JoinHostPort(ip.String(), port),
				)
				if e == nil {
					return c, nil
				}
				last = e
			}
			if last == nil {
				last = errors.New("tracker has no addresses")
			}
			return nil, last
		},
	}

	return &Client{
		http: &http.Client{
			Transport: t,
			CheckRedirect: func(r *http.Request, via []*http.Request) error {
				if len(via) >= 3 {
					return errors.New("too many redirects")
				}
				_, e := ValidateURL(r.URL.String())
				return e
			},
		},
	}
}
func (c *Client) Close() { c.http.CloseIdleConnections() }

type capReader struct {
	r    io.Reader
	left int64
}

func (r *capReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		var one [1]byte

		n, e := r.r.Read(one[:])
		if n > 0 {
			return 0, errors.New("tracker response exceeds byte limit")
		}

		return 0, e
	}

	if int64(len(p)) > r.left {
		p = p[:r.left]
	}

	n, e := r.r.Read(p)
	r.left -= int64(n)

	return n, e
}

func (c *Client) body(ctx context.Context, raw string, l Limits) (*bodyReader, error) {
	if _, e := ValidateURL(raw); e != nil {
		return nil, e
	}

	req, e := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if e != nil {
		return nil, e
	}

	req.Header.Set("User-Agent", "bitmagnet-tracker/1.0")
	req.Header.Set("Accept-Encoding", "gzip")

	res, e := c.http.Do(req)
	if e != nil {
		return nil, e
	}

	if res.StatusCode != http.StatusOK {
		_ = res.Body.Close()

		delay := time.Duration(0)
		if n, e := strconv.Atoi(res.Header.Get("Retry-After")); e == nil && n > 0 {
			delay = time.Duration(min(n, 604800)) * time.Second
		} else if t, e := http.ParseTime(res.Header.Get("Retry-After")); e == nil {
			delay = time.Until(t)
		}

		return nil, HTTPError{Status: res.StatusCode, RetryAfter: delay}
	}

	wire := &capReader{r: res.Body, left: l.WireBytes}

	var r io.Reader = wire

	switch strings.ToLower(res.Header.Get("Content-Encoding")) {
	case "gzip":
		g, e := gzip.NewReader(r)
		if e != nil {
			_ = res.Body.Close()
			return nil, e
		}

		r = g
	case "", "identity":
	default:
		_ = res.Body.Close()
		return nil, errors.New("unsupported tracker encoding")
	}

	decoded := &capReader{r: r, left: l.DecodedBytes}

	return &bodyReader{Reader: decoded, Closer: res.Body, wire: wire, decoded: decoded}, nil
}

type bodyReader struct {
	wire, decoded *capReader
	io.Reader
	io.Closer
}

// Scrape invokes emit for each complete validated entry. Any error means a partial
// or failed run; the caller must not infer absence from an incomplete catalogue.
func (c *Client) Scrape(
	ctx context.Context,
	raw string,
	l Limits,
	emit func(Entry) error,
) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, l.Timeout)
	defer cancel()

	u, e := ValidateURL(raw)
	if e != nil {
		return 0, e
	}

	if _, ok := u.Query()["info_hash"]; ok {
		return 0, errors.New("full scrape must not specify info_hash")
	}

	body, e := c.body(ctx, raw, l)
	if e != nil {
		return 0, e
	}

	defer body.Close()

	if l.OnRead != nil {
		defer func() { l.OnRead(l.WireBytes-body.wire.left, l.DecodedBytes-body.decoded.left) }()
	}

	return ParseScrape(body, l.Hashes, emit)
}

func ParseScrape(r io.Reader, maxHashes int, emit func(Entry) error) (count int, err error) {
	d := decoder{bufio.NewReader(r)}
	if err = d.expect('d'); err != nil {
		return
	}

	found := false
	keys := map[string]bool{}

	for {
		var end bool

		end, err = d.end()
		if err != nil {
			return
		}

		if end {
			break
		}

		var key string

		key, err = d.str()
		if err != nil {
			return
		}

		if keys[key] {
			return count, errors.New("duplicate root key")
		}

		keys[key] = true
		if len(keys) > 32 {
			return count, errors.New("too many root fields")
		}

		if key != "files" {
			var v any

			v, err = d.value(0)
			if err != nil {
				return
			}

			if key == "failure reason" || key == "failure_reason" {
				return count, fmt.Errorf("tracker rejected scrape: %.200v", v)
			}

			continue
		}

		found = true

		if err = d.expect('d'); err != nil {
			return
		}

		for {
			end, err = d.end()
			if err != nil {
				return
			}

			if end {
				break
			}

			if count >= maxHashes {
				return count, errors.New("scrape hash limit reached")
			}

			var hash string

			hash, err = d.str()
			if err != nil {
				return
			}

			if len(hash) != 20 {
				return count, errors.New("invalid scrape hash length")
			}

			var value any

			value, err = d.value(0)
			if err != nil {
				return
			}

			m, ok := value.(map[string]any)
			if !ok {
				return count, errors.New("invalid scrape statistics")
			}

			vals := make([]int64, 3)

			for i, k := range []string{"complete", "incomplete", "downloaded"} {
				n, ok := m[k].(int64)
				if !ok || n < 0 || n > 2147483647 {
					return count, errors.New("invalid scrape counter")
				}

				vals[i] = n
			}

			var id protocol.ID

			copy(id[:], hash)

			entry := Entry{Hash: id, Seeders: vals[0], Leechers: vals[1], Downloaded: vals[2]}
			if err = emit(entry); err != nil {
				return
			}

			count++
		}
	}

	if !found {
		return count, errors.New("no files dictionary")
	}

	if _, e := d.r.ReadByte(); e != io.EOF {
		if e != nil {
			return count, e
		}

		return count, errors.New("trailing scrape bytes")
	}

	return count, nil
}

type AnnounceResult struct {
	Peers    []netip.AddrPort
	Interval time.Duration
}

// Announce follows the pinned anacrolix convention left=-1 before metadata exists.
// It never advertises a completed download. Use the same ID and port for stopped.
func (c *Client) Announce(
	ctx context.Context,
	raw string,
	hash, peerID protocol.ID,
	port uint16,
	event string,
) (AnnounceResult, error) {
	var out AnnounceResult
	if event != "started" && event != "stopped" {
		return out, errors.New("invalid metadata session event")
	}

	u, e := ValidateURL(raw)
	if e != nil {
		return out, e
	}

	q := u.Query()
	q.Set("info_hash", string(hash[:]))
	q.Set("peer_id", string(peerID[:]))
	q.Set("port", strconv.Itoa(int(port)))
	q.Set("uploaded", "0")
	q.Set("downloaded", "0")
	q.Set("left", "-1")
	q.Set("compact", "1")
	q.Set("numwant", "50")
	q.Set("event", event)
	u.RawQuery = q.Encode()

	body, e := c.body(ctx, u.String(), Limits{WireBytes: 65536, DecodedBytes: 65536})
	if e != nil {
		return out, e
	}

	defer body.Close()
	// Compact peers can exceed the entry parser's 4KiB string bound; numwant=50 keeps ordinary replies small.
	d := decoder{bufio.NewReader(body)}

	v, e := d.value(0)
	if e != nil {
		return out, e
	}

	m, ok := v.(map[string]any)
	if !ok {
		return out, errors.New("invalid announce response")
	}

	for _, k := range []string{"failure reason", "failure_reason"} {
		if reason, ok := m[k]; ok {
			return out, fmt.Errorf("tracker rejected announce: %.200v", reason)
		}
	}

	if interval, ok := m["interval"].(int64); ok && interval > 0 {
		out.Interval = time.Duration(min(interval, 604800)) * time.Second
	}

	add := func(ip string, port int64) {
		a, e := netip.ParseAddr(ip)
		if e == nil && PublicAddress(a) && port > 0 && port <= 65535 && len(out.Peers) < 50 {
			out.Peers = append(out.Peers, netip.AddrPortFrom(a, uint16(port)))
		}
	}

	switch peers := m["peers"].(type) {
	case string:
		if len(peers)%6 != 0 {
			return out, errors.New("invalid compact peer list")
		}

		for i := 0; i < len(peers); i += 6 {
			a := netip.AddrFrom4([4]byte{peers[i], peers[i+1], peers[i+2], peers[i+3]})
			add(a.String(), int64(peers[i+4])<<8|int64(peers[i+5]))
		}
	case []any:
		for _, p := range peers {
			if pm, ok := p.(map[string]any); ok {
				ip, _ := pm["ip"].(string)
				port, _ := pm["port"].(int64)
				add(ip, port)
			}
		}
	case nil:
	default:
		return out, errors.New("invalid peers value")
	}

	return out, nil
}
