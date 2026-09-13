package metainfo

import (
	"testing"

	"github.com/anacrolix/torrent/bencode"
	ami "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestVerifiedMetadataLengths(t *testing.T) {
	t.Parallel()

	valid := Info{Name: "test.bin", Length: 1024, PieceLength: 16384, Pieces: make([]byte, 20)}
	raw, e := bencode.Marshal(valid)
	require.NoError(t, e)
	_, e = ParseMetaInfoBytes(protocol.ID(ami.HashBytes(raw)), raw)
	require.NoError(t, e)
	_, e = ParseMetaInfoBytes(protocol.RandomNodeID(), raw)
	require.ErrorContains(t, e, "wrong hash")

	for _, bad := range []Info{
		{Length: -1, PieceLength: 16},
		{Length: 1024, PieceLength: 0},
		{Length: 1024, PieceLength: 16, Pieces: make([]byte, 20)},
		{PieceLength: 16, Files: []ami.FileInfo{{Length: -1}}},
	} {
		b, e := bencode.Marshal(bad)
		require.NoError(t, e)
		_, e = ParseMetaInfoBytes(protocol.ID(ami.HashBytes(b)), b)
		require.Error(t, e)
	}
}
