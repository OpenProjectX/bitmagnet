package dhtcrawler

import (
	"context"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/torrentwriter"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gen"
	"gorm.io/gorm/clause"
)

// runPersistTorrents waits on the persistTorrents channel, and persists torrents to the database in batches.
// After persisting each batch it will publish a message to the classifier,
// and forward the hash on the scrape channel to attempt finding the seeders/leechers.
func (c *crawler) runPersistTorrents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case items := <-c.persistTorrents.Out():
			inputs := make([]torrentwriter.Input, 0, len(items))

			byHash := make(map[protocol.ID]nodeHasPeersForHash)
			for _, item := range items {
				if _, ok := byHash[item.infoHash]; ok {
					continue
				}

				byHash[item.infoHash] = item.nodeHasPeersForHash
				inputs = append(
					inputs,
					torrentwriter.Input{
						Hash:       item.infoHash,
						Info:       item.metaInfo,
						Source:     "dht",
						SourceName: "DHT",
					},
				)
			}

			hashes, err := torrentwriter.Write(
				ctx,
				c.dao.Torrent.WithContext(ctx).UnderlyingDB(),
				c.blockingManager,
				inputs,
				torrentwriter.Options{
					SaveFilesThreshold: c.saveFilesThreshold,
					SavePieces:         c.savePieces,
					ClassifyDelay:      time.Minute,
				},
			)
			if err != nil {
				c.logger.Errorf("error persisting torrents: %s", err)
				continue
			}

			c.persistedTotal.With(prometheus.Labels{"entity": "Torrent"}).Add(float64(len(hashes)))

			for _, hash := range hashes {
				select {
				case <-ctx.Done():
					return
				case c.scrape.In() <- byHash[hash]:
				}
			}
		}
	}
}

// runPersistSources waits on the persistSources channel for scraped torrents, and persists sources
// (which includes discovery date, seeders and leechers) to the database in batches.
func (c *crawler) runPersistSources(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case scrapes := <-c.persistSources.Out():
			srcs := make([]*model.TorrentsTorrentSource, 0, len(scrapes))

			hashSet := make(map[protocol.ID]struct{}, len(scrapes))
			for _, s := range scrapes {
				if _, ok := hashSet[s.infoHash]; ok {
					continue
				}

				hashSet[s.infoHash] = struct{}{}

				if src, err := createTorrentSourceModel(s); err != nil {
					c.logger.Errorf("error creating torrent source model: %s", err.Error())
				} else {
					srcs = append(srcs, &src)
				}
			}

			if persistErr := c.dao.WithContext(ctx).TorrentsTorrentSource.Clauses(
				clause.OnConflict{
					Columns: []clause.Column{
						{Name: string(c.dao.TorrentsTorrentSource.InfoHash.ColumnName())},
						{Name: string(c.dao.TorrentsTorrentSource.Source.ColumnName())},
					},
					DoUpdates: clause.AssignmentColumns([]string{
						string(c.dao.TorrentsTorrentSource.Seeders.ColumnName()),
						string(c.dao.TorrentsTorrentSource.Leechers.ColumnName()),
						// sets to null, fixes torrents indexed before 0.8.0 with published_at
						// 0001-01-01 00:00:00+00:
						string(c.dao.TorrentsTorrentSource.PublishedAt.ColumnName()),
						string(c.dao.TorrentsTorrentSource.UpdatedAt.ColumnName()),
					}),
				},
			).Where(
				// check that the torrent record hasn't been deleted:
				gen.Exists(c.dao.WithContext(ctx).Torrent.Where(
					c.dao.Torrent.InfoHash.EqCol(c.dao.TorrentsTorrentSource.InfoHash),
				)),
			).CreateInBatches(srcs, 100); persistErr != nil {
				c.logger.Errorf("error persisting torrent sources: %s", persistErr.Error())
			} else {
				c.persistedTotal.With(prometheus.Labels{"entity": "TorrentsTorrentSource"}).Add(float64(len(srcs)))
				c.logger.Debugw("persisted torrent sources", "count", len(srcs))
			}
		}
	}
}

func createTorrentSourceModel(
	result infoHashWithScrape,
) (model.TorrentsTorrentSource, error) {
	seeders := model.NewNullUint(uint(result.bfsd.ApproximatedSize()))
	leechers := model.NewNullUint(uint(result.bfpe.ApproximatedSize()))

	return model.TorrentsTorrentSource{
		Source:   "dht",
		InfoHash: result.infoHash,
		Seeders:  seeders,
		Leechers: leechers,
	}, nil
}
