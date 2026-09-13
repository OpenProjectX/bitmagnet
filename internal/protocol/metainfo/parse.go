package metainfo

import (
	"errors"
	"fmt"
	"math"

	"github.com/anacrolix/torrent/bencode"
	mi "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

func ParseMetaInfoBytes(infoHash protocol.ID, metaInfoBytes []byte) (Info, error) {
	if protocol.ID(mi.HashBytes(metaInfoBytes)) != infoHash {
		return Info{}, errors.New("info bytes have wrong hash")
	}

	var info Info
	if unmarshalErr := bencode.Unmarshal(metaInfoBytes, &info); unmarshalErr != nil {
		return Info{}, fmt.Errorf("error unmarshaling info bytes: %w", unmarshalErr)
	}

	// A valid hash authenticates bytes, not their semantic correctness. Reject
	// lengths that could wrap when models convert signed protocol values to uint.
	total := info.Length
	if total < 0 || info.PieceLength <= 0 || len(info.Pieces)%20 != 0 {
		return Info{}, errors.New("invalid v1 torrent lengths or piece hashes")
	}

	if len(info.Files) > 0 {
		if total != 0 {
			return Info{}, errors.New("torrent has both single and multi-file lengths")
		}

		for _, file := range info.Files {
			if file.Length < 0 || file.Length > math.MaxInt64-total {
				return Info{}, errors.New("invalid torrent file length")
			}

			total += file.Length
		}
	}

	expected := total / info.PieceLength
	if total%info.PieceLength != 0 {
		expected++
	}

	if int64(len(info.Pieces)/20) != expected {
		return Info{}, errors.New("piece hashes do not cover torrent length")
	}
	return info, nil
}
