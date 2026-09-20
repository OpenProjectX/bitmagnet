// Package torrentwriter persists verified metadata from any discovery source.
package torrentwriter

import (
	"context"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Input struct {
	Hash               protocol.ID
	Info               metainfo.Info // caller must verify raw info bytes before constructing Input
	Source, SourceName string
	Seeders, Leechers  model.NullUint
	ObservedAt         time.Time
}
type Options struct {
	SaveFilesThreshold uint
	SavePieces         bool
	ClassifyDelay      time.Duration
}

// NeedsMetadata matches the DHT crawler's file-retention triage.
func NeedsMetadata(t model.Torrent, threshold uint) bool {
	return t.FilesStatus == model.FilesStatusNoInfo ||
		(t.FilesStatus != model.FilesStatusSingle && !t.FilesCount.Valid) ||
		(t.FilesStatus == model.FilesStatusOverThreshold && t.FilesCount.Uint <= threshold)
}

// Write couples metadata and downstream jobs. A DB advisory lock serializes
// automatic ingestion with deletion tombstones across processes (not network IO).
func Write(
	ctx context.Context,
	db *gorm.DB,
	blocks blocking.Manager,
	inputs []Input,
	opts Options,
) ([]protocol.ID, error) {
	hashes := make([]protocol.ID, 0, len(inputs))
	for _, in := range inputs {
		hashes = append(hashes, in.Hash)
	}

	allowed, err := blocks.Filter(ctx, hashes)
	if err != nil {
		return nil, err
	}

	allow := make(map[protocol.ID]bool, len(allowed))
	for _, h := range allowed {
		allow[h] = true
	}

	if len(allowed) == 0 {
		return nil, nil
	}

	var accepted []protocol.ID

	// Set Context in the same Session as NewDB. A subsequent WithContext call
	// changes GORM's clone mode and preserves the caller's model/table scope.
	err = db.Session(&gorm.Session{NewDB: true, Context: ctx}).
		Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(72619401)").Error; err != nil {
				return err
			}

			// Read eligibility in batches and preserve the original DHT batch-write path.
			var tombstones []struct{ InfoHash protocol.ID }
			if err := tx.
				Table("torrent_discovery_tombstones").
				Where("info_hash IN ?", hashes).
				Find(&tombstones).Error; err != nil {
				return err
			}

			for _, t := range tombstones {
				delete(allow, t.InfoHash)
			}

			var previous []model.Torrent
			if err := tx.Select("info_hash", "files_status", "files_count").
				Where("info_hash IN ?", hashes).
				Find(&previous).Error; err != nil {
				return err
			}

			existing := make(map[protocol.ID]model.Torrent, len(previous))
			for _, t := range previous {
				existing[t.InfoHash] = t
			}

			var classify []protocol.ID

			var torrents []model.Torrent

			var files []model.TorrentFile

			var pieces []model.TorrentPieces

			var sources []model.TorrentSource

			var observations, freshObservations []model.TorrentsTorrentSource

			sourceSeen := map[string]bool{}

			for _, in := range inputs {
				if !allow[in.Hash] {
					continue
				}

				delete(allow, in.Hash)
				old, exists := existing[in.Hash]

				t, err := Model(
					in.Hash,
					in.Info,
					opts.SavePieces,
					in.Source,
					opts.SaveFilesThreshold,
				)
				if err != nil {
					return err
				}

				if !sourceSeen[in.Source] {
					sources = append(
						sources,
						model.TorrentSource{Key: in.Source, Name: in.SourceName},
					)
					sourceSeen[in.Source] = true
				}

				if !exists || NeedsMetadata(old, opts.SaveFilesThreshold) {
					files = append(files, t.Files...)
					torrents = append(torrents, t)
					classify = append(classify, in.Hash)
				}

				if opts.SavePieces {
					pieces = append(pieces, t.Pieces)
				}

				src := t.Sources[0]
				src.Seeders = in.Seeders
				src.Leechers = in.Leechers

				if !in.ObservedAt.IsZero() {
					src.UpdatedAt = in.ObservedAt
				}

				if in.Seeders.Valid || in.Leechers.Valid {
					freshObservations = append(freshObservations, src)
				} else {
					observations = append(observations, src)
				}

				accepted = append(accepted, in.Hash)
			}

			if len(sources) > 0 {
				if err := tx.
					Clauses(clause.OnConflict{DoNothing: true}).
					CreateInBatches(sources, 100).Error; err != nil {
					return err
				}
			}

			if len(torrents) > 0 {
				if err := tx.Omit(clause.Associations).
					Clauses(clause.OnConflict{
						Columns: []clause.Column{{Name: "info_hash"}},
						DoUpdates: clause.AssignmentColumns([]string{
							"name", "size", "private", "files_status", "files_count", "updated_at",
						}),
					}).
					CreateInBatches(torrents, 100).Error; err != nil {
					return err
				}
			}

			if len(files) > 0 {
				if err := tx.
					Clauses(clause.OnConflict{DoNothing: true}).
					CreateInBatches(files, 100).Error; err != nil {
					return err
				}
			}

			if len(pieces) > 0 {
				if err := tx.
					Clauses(clause.OnConflict{DoNothing: true}).
					CreateInBatches(pieces, 10).Error; err != nil {
					return err
				}
			}

			if len(observations) > 0 {
				if err := tx.Omit(clause.Associations).
					Clauses(clause.OnConflict{DoNothing: true}).
					CreateInBatches(observations, 100).Error; err != nil {
					return err
				}
			}

			if len(freshObservations) > 0 {
				if err := tx.Omit(clause.Associations).
					Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "info_hash"}, {Name: "source"}},
						DoUpdates: clause.AssignmentColumns([]string{"seeders", "leechers", "updated_at"}),
						Where: clause.Where{Exprs: []clause.Expression{
							clause.Expr{SQL: "torrents_torrent_sources.updated_at <= EXCLUDED.updated_at"},
						}},
					}).
					CreateInBatches(freshObservations, 100).Error; err != nil {
					return err
				}
			}

			for len(classify) > 0 {
				n := min(100, len(classify))

				job, err := processor.NewQueueJob(
					processor.MessageParams{InfoHashes: classify[:n]},
					model.QueueJobDelayBy(opts.ClassifyDelay),
				)
				if err != nil {
					return err
				}

				if err = tx.Create(&job).Error; err != nil {
					return err
				}

				classify = classify[n:]
			}

			return nil
		})
	if err != nil {
		return nil, err
	}

	return accepted, nil
}
