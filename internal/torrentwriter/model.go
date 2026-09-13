package torrentwriter

import (
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
)

func Model(
	hash protocol.ID,
	info metainfo.Info,
	savePieces bool,
	source string,
	saveFilesThreshold uint,
) (model.Torrent, error) {
	name := info.BestName()

	private := false
	if info.Private != nil {
		private = *info.Private
	}

	var filesCount model.NullUint

	filesStatus := model.FilesStatusSingle
	if len(info.Files) > 0 {
		filesStatus = model.FilesStatusMulti
		filesCount = model.NewNullUint(uint(len(info.Files)))
	}

	files := make([]model.TorrentFile, 0, min(int(saveFilesThreshold), len(info.Files)))

	for i, file := range info.Files {
		if i >= int(saveFilesThreshold) {
			filesStatus = model.FilesStatusOverThreshold
			break
		}

		files = append(files, model.TorrentFile{
			InfoHash: hash,
			Index:    uint(i),
			Path:     file.DisplayPath(&info),
			Size:     uint(file.Length),
		})
	}

	var pieces model.TorrentPieces
	if savePieces {
		pieces = model.TorrentPieces{
			InfoHash:    hash,
			PieceLength: info.PieceLength,
			Pieces:      info.Pieces,
		}
	}

	return model.Torrent{
		InfoHash:    hash,
		Name:        name,
		Size:        uint(info.TotalLength()),
		Private:     private,
		Pieces:      pieces,
		Files:       files,
		FilesStatus: filesStatus,
		FilesCount:  filesCount,
		Sources: []model.TorrentsTorrentSource{
			{
				Source:   source,
				InfoHash: hash,
			},
		},
	}, nil
}
