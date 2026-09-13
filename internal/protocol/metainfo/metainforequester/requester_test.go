package metainforequester

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	ami "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/require"
)

func message(piece int, data []byte) []byte {
	h, _ := bencode.Marshal(extDict{MsgType: 1, Piece: piece})
	body := append([]byte{20, 1}, h...)
	body = append(body, data...)

	return append(uintToBigEndian4(uint(len(body))), body...)
}

func TestMetadataAssembly(t *testing.T) {
	t.Parallel()

	full := bytes.Repeat([]byte{'a'}, 16384)
	last := []byte("last")

	for _, tt := range []struct {
		name    string
		packets []byte
		bad     bool
	}{
		{"out_of_order", append(message(1, last), message(0, full)...), false},
		{"duplicate", append(append(message(0, full), message(0, full)...), message(1, last)...), false},
		{"negative", message(-1, full), true},
		{"outside", message(2, last), true},
		{"short", message(0, last), true},
		{"conflict", append(message(0, full), message(0, bytes.Repeat([]byte{'b'}, 16384))...), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, e := readAllPieces(bytes.NewReader(tt.packets), 16388)
			if tt.bad {
				require.Error(t, e)
			} else {
				require.NoError(t, e)
				require.Equal(t, append(append([]byte{}, full...), last...), b)
			}
		})
	}
}

type badRW struct{}

func (badRW) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func (badRW) Read([]byte) (int, error)  { panic("must return write error before reading") }
func TestExtensionWriteFailure(t *testing.T) {
	t.Parallel()

	_, _, e := exHandshake(badRW{})
	require.ErrorContains(t, e, "write failed")
}

func TestRequesterMetadataOnly(t *testing.T) {
	t.Parallel()

	info := ami.Info{Name: "fixture-file", Length: 1024, PieceLength: 16384, Pieces: make([]byte, 20)}
	raw, e := bencode.Marshal(info)
	require.NoError(t, e)

	hash := protocol.ID(ami.HashBytes(raw))
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, e)

	defer listener.Close()

	serverErr := make(chan error, 1)

	go func() {
		conn, e := listener.Accept()
		if e != nil {
			serverErr <- e
			return
		}

		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))

		handshake := make([]byte, 68)
		if _, e = io.ReadFull(conn, handshake); e != nil {
			serverErr <- e
			return
		}

		if _, e = conn.Write(handshake); e != nil {
			serverErr <- e
			return
		}

		if _, e = readMessage(conn); e != nil {
			serverErr <- e
			return
		}

		h, _ := bencode.Marshal(
			map[string]any{"m": map[string]int{"ut_metadata": 3}, "metadata_size": len(raw)},
		)
		packet := append([]byte{20, 0}, h...)

		_, e = conn.Write(append(uintToBigEndian4(uint(len(packet))), packet...))
		if e != nil {
			serverErr <- e
			return
		}

		request, e := readMessage(conn)
		if e != nil {
			serverErr <- e
			return
		}

		if len(request) < 2 || request[0] != 20 || request[1] != 3 {
			serverErr <- errors.New("not negotiated metadata request")
			return
		}

		_, e = conn.Write(message(0, raw))
		if e != nil {
			serverErr <- e
			return
		}
		// No payload request may follow successful metadata. The requester closes.
		var b [1]byte

		n, e := conn.Read(b[:])
		if n != 0 || e == nil {
			serverErr <- errors.New("unexpected payload request or open connection")
			return
		}
		serverErr <- nil
	}()

	r := requester{clientID: protocol.RandomPeerID(), timeout: time.Second, dialer: &net.Dialer{}}
	res, e := r.Request(context.Background(), hash, listener.Addr().(*net.TCPAddr).AddrPort())
	require.NoError(t, e)
	require.Equal(t, info.Name, res.Info.Name)
	require.NoError(t, <-serverErr)
}

func TestCancellationClosesPeer(t *testing.T) {
	t.Parallel()

	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, e)

	defer listener.Close()

	connected := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		conn, e := listener.Accept()
		if e != nil {
			return
		}

		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))

		var h [68]byte
		_, _ = io.ReadFull(conn, h[:])

		close(connected)

		var p [4]byte
		_, _ = io.ReadFull(conn, p[:])
		_ = binary.BigEndian.Uint32(p[:])
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { <-connected; cancel() }()

	r := requester{timeout: time.Minute, dialer: &net.Dialer{}}
	start := time.Now()
	_, e = r.Request(ctx, protocol.RandomNodeID(), netip.MustParseAddrPort(listener.Addr().String()))
	require.Error(t, e)
	require.Less(t, time.Since(start), time.Second)
	<-done
}
