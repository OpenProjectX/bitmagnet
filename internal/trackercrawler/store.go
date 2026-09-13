package trackercrawler

import (
	"context"
	"errors"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/torrentwriter"
	"github.com/bitmagnet-io/bitmagnet/internal/tracker"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	stateBlocked   = "blocked"
	stateResolved  = "resolved"
	stateRetryWait = "retry_wait"
)

var (
	ErrCapacity = errors.New("tracker staging capacity reached")
	ErrLease    = errors.New("tracker work lease lost")
)

type store struct {
	db     *gorm.DB
	blocks blocking.Manager
	config Config
	files  uint
	pieces bool
}
type work struct {
	InfoHash   protocol.ID
	LeaseToken string
	Attempts   int
}
type observation struct {
	TrackerID string
	InfoHash  protocol.ID
	Seeders   int64
	Leechers  int64
	LastSeen  time.Time
}

func (s store) sync(ctx context.Context) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, ep := range s.config.Endpoints {
			scrape := ep.ScrapeURL
			if scrape == "" {
				scrape, _ = tracker.ScrapeURL(ep.AnnounceURL)
			}

			if e := tx.Exec(`INSERT INTO tracker_endpoints(id,announce_url,scrape_url) VALUES (?,?,?)
   ON CONFLICT(id) DO UPDATE SET announce_url=EXCLUDED.announce_url,scrape_url=EXCLUDED.scrape_url`,
				ep.ID, ep.AnnounceURL, scrape,
			).Error; e != nil {
				return e
			}

			src := model.TorrentSource{Key: "tracker:" + ep.ID, Name: "Tracker: " + ep.ID}
			if e := tx.
				Clauses(clause.OnConflict{DoNothing: true}).Create(&src).Error; e != nil {
				return e
			}
		}

		return nil
	})
}

func (s store) beginRun(ctx context.Context, ep Endpoint) (int64, string, error) {
	token := protocol.RandomNodeID().String()

	var id int64

	e := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Exec(
			`UPDATE tracker_endpoints SET lease_token=?,lease_until=?,next_run=? WHERE id=? AND next_run<=now() AND
(lease_until IS NULL OR lease_until<now())`,
			token,
			time.Now().Add(2*s.config.ScrapeTimeout),
			time.Now().Add(s.config.FullScrapeInterval),
			ep.ID,
		)
		if res.Error != nil {
			return res.Error
		}

		if res.RowsAffected == 0 {
			return nil
		}

		return tx.Raw("INSERT INTO tracker_scrape_runs(tracker_id) VALUES (?) RETURNING id", ep.ID).
			Scan(&id).
			Error
	})

	return id, token, e
}

type scrapeBytes struct{ wire, decoded int64 }

func (s store) endRun(
	ctx context.Context,
	ep Endpoint,
	id int64,
	token string,
	count, admitted int,
	bytes scrapeBytes,
	runErr error,
) error {
	status := "complete"
	capability := "empty"
	message := ""
	delay := s.config.FullScrapeInterval

	if count > 0 {
		capability = "hashes_observed"
	}

	if runErr != nil {
		status = "failed"
		if count > 0 {
			status = "partial"
		}

		message = runErr.Error()
		if len(message) > 500 {
			message = message[:500]
		}

		if count == 0 {
			capability = "unconfirmed"
		}

		var h tracker.HTTPError
		if errors.As(runErr, &h) && h.RetryAfter > delay {
			delay = h.RetryAfter
		}
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Exec(`UPDATE tracker_scrape_runs SET finished_at=now(),status=?,entries=?,admitted=?,
 wire_bytes=?,decoded_bytes=?,error=? WHERE
id=?`,
			status, count, admitted, bytes.wire, bytes.decoded, message, id,
		).Error; e != nil {
			return e
		}

		return tx.Exec(
			`UPDATE tracker_endpoints SET lease_token=NULL,lease_until=NULL,capability=?,last_error=?,
   failures=CASE WHEN ? THEN failures+1 ELSE 0 END,next_run=?,updated_at=now() WHERE id=? AND lease_token=?`,
			capability,
			message,
			runErr != nil,
			time.Now().Add(delay),
			ep.ID,
			token,
		).Error
	})
}

func (s store) admit(ctx context.Context, ep Endpoint, entries []tracker.Entry) (int, error) {
	hashes := make([]protocol.ID, 0, len(entries))
	for _, e := range entries {
		hashes = append(hashes, e.Hash)
	}

	allowed, e := s.blocks.Filter(ctx, hashes)
	if e != nil {
		return 0, e
	}

	allow := map[protocol.ID]bool{}
	for _, h := range allowed {
		allow[h] = true
	}

	admitted := 0

	e = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Exec("SELECT pg_advisory_xact_lock(72619402)").Error; e != nil {
			return e
		}

		var pending, total int64
		if e := tx.
			Table("metadata_resolution_work").
			Where("state IN ('pending','leased','retry_wait')").
			Count(&pending).Error; e != nil {
			return e
		}

		if e := tx.
			Table("tracker_hash_observations").
			Count(&total).Error; e != nil {
			return e
		}

		for _, entry := range entries {
			if !allow[entry.Hash] {
				continue
			}

			delete(allow, entry.Hash)

			var tomb int64
			if e := tx.
				Table("torrent_discovery_tombstones").
				Where("info_hash = ?", entry.Hash).
				Count(&tomb).Error; e != nil {
				return e
			}

			if tomb > 0 {
				continue
			}

			var t model.Torrent
			e := tx.
				Where("info_hash = ?", entry.Hash).Take(&t).Error
			exists := e == nil

			if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
				return e
			}

			if exists && t.Private {
				continue
			}

			var known int64
			if e := tx.
				Table("tracker_hash_observations").
				Where("tracker_id=? AND info_hash=?", ep.ID, entry.Hash).
				Count(&known).Error; e != nil {
				return e
			}

			if total >= int64(5*s.config.MaxPendingHashes) && known == 0 {
				return ErrCapacity
			}

			if !exists || torrentwriter.NeedsMetadata(t, s.files) {
				var n int64
				if e := tx.
					Table("metadata_resolution_work").
					Where("info_hash=?", entry.Hash).
					Count(&n).Error; e != nil {
					return e
				}

				if n == 0 && pending >= int64(s.config.MaxPendingHashes) {
					return ErrCapacity
				}

				res := tx.Exec(
					"INSERT INTO metadata_resolution_work(info_hash) VALUES (?) ON CONFLICT DO NOTHING",
					entry.Hash,
				)
				if res.Error != nil {
					return res.Error
				}

				admitted += int(res.RowsAffected)
				pending += res.RowsAffected
			}

			if e := tx.Exec(`INSERT INTO tracker_hash_observations(tracker_id,info_hash,seeders,leechers,downloaded)
 VALUES (?,?,?,?,?)
    ON CONFLICT(tracker_id,info_hash) DO UPDATE SET
seeders=EXCLUDED.seeders,leechers=EXCLUDED.leechers,downloaded=EXCLUDED.downloaded,last_seen=now()`,
				ep.ID, entry.Hash, entry.Seeders, entry.Leechers, entry.Downloaded,
			).Error; e != nil {
				return e
			}

			if known == 0 {
				total++
			}

			if exists {
				if e := refreshSource(tx, observation{
					TrackerID: ep.ID, InfoHash: entry.Hash,
					Seeders: entry.Seeders, Leechers: entry.Leechers, LastSeen: time.Now(),
				}); e != nil {
					return e
				}
			}
		}

		return nil
	})
	if e != nil {
		return 0, e
	}

	return admitted, nil
}

func refreshSource(tx *gorm.DB, o observation) error {
	// INSERT SELECT will not recreate a torrent that was concurrently deleted.
	if e := tx.Exec(`INSERT INTO torrents_torrent_sources(info_hash,source,seeders,leechers,created_at,updated_at)
  SELECT info_hash,?,?,?,now(),? FROM torrents WHERE info_hash=? AND NOT private
  ON CONFLICT(info_hash,source) DO UPDATE SET
seeders=EXCLUDED.seeders,leechers=EXCLUDED.leechers,updated_at=EXCLUDED.updated_at
  WHERE torrents_torrent_sources.updated_at<=EXCLUDED.updated_at`,
		"tracker:"+o.TrackerID, o.Seeders, o.Leechers, o.LastSeen, o.InfoHash,
	).Error; e != nil {
		return e
	}

	return tx.Exec(
		`UPDATE torrent_contents SET seeders=(SELECT max(seeders) FROM torrents_torrent_sources WHERE info_hash=?),
 leechers=(SELECT max(leechers) FROM torrents_torrent_sources WHERE info_hash=?) WHERE info_hash=?`,
		o.InfoHash,
		o.InfoHash,
		o.InfoHash,
	).Error
}

func (s store) claim(ctx context.Context) (work, error) {
	var w work

	token := protocol.RandomNodeID().String()

	ids := make([]string, 0, len(s.config.Endpoints))
	for _, ep := range s.config.Endpoints {
		ids = append(ids, ep.ID)
	}

	e := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Raw(`SELECT w.info_hash,w.attempts FROM metadata_resolution_work w WHERE
   ((state IN ('pending','retry_wait') AND next_attempt<=now()) OR (state='leased' AND lease_until<now()))
   AND EXISTS (SELECT 1 FROM tracker_hash_observations o WHERE o.info_hash=w.info_hash AND o.tracker_id IN ?)
   ORDER BY next_attempt FOR UPDATE SKIP LOCKED LIMIT 1`, ids).Scan(&w)
		if res.Error != nil {
			return res.Error
		}

		if res.RowsAffected == 0 {
			return nil
		}

		w.LeaseToken = token
		w.Attempts++

		return tx.Exec(
			`UPDATE metadata_resolution_work SET
state='leased',attempts=?,lease_token=?,lease_until=?,updated_at=now() WHERE info_hash=?`,
			w.Attempts,
			token,
			time.Now().Add(s.config.ResolveTimeout+30*time.Second),
			w.InfoHash,
		).Error
	})

	return w, e
}

func (s store) origins(ctx context.Context, h protocol.ID) ([]observation, error) {
	var origins []observation
	e := s.db.WithContext(ctx).
		Table("tracker_hash_observations").
		Where("info_hash=?", h).
		Order("seeders DESC, last_seen DESC").
		Find(&origins).
		Error

	return origins, e
}

func (s store) finish(
	ctx context.Context,
	w work,
	state string,
	reason error,
	input *torrentwriter.Input,
	origins []observation,
	next time.Time,
) error {
	// Filter before acquiring DB locks; blocking.Manager may flush its own transaction.
	if input != nil {
		ok, e := s.blocks.Filter(ctx, []protocol.ID{w.InfoHash})
		if e != nil {
			return e
		}

		if len(ok) == 0 {
			state = stateBlocked
			input = nil
		}
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var n int64
		if e := tx.Raw(`SELECT count(*) FROM (SELECT 1 FROM metadata_resolution_work WHERE info_hash=? AND
lease_token=? AND lease_until>now() FOR UPDATE) l`,
			w.InfoHash, w.LeaseToken).Scan(&n).Error; e != nil {
			return e
		}

		if n == 0 {
			return ErrLease
		}

		if input != nil {
			// Avoid a second blocking-manager flush while holding row locks. The writer
			// checks exact tombstones under the deletion lock at commit.
			hashes, e := torrentwriter.Write(
				ctx,
				tx,
				allowOne{},
				[]torrentwriter.Input{*input},
				torrentwriter.Options{SaveFilesThreshold: s.files, SavePieces: s.pieces},
			)
			if e != nil {
				return e
			}

			if len(hashes) == 0 {
				state = stateBlocked
			} else {
				for _, o := range origins {
					if e := refreshSource(tx, o); e != nil {
						return e
					}
				}
			}
		}

		message := ""
		if reason != nil {
			message = reason.Error()
			if len(message) > 500 {
				message = message[:500]
			}
		}

		return tx.Exec(
			`UPDATE metadata_resolution_work SET
state=?,lease_token=NULL,lease_until=NULL,last_error=?,next_attempt=?,updated_at=now() WHERE
info_hash=? AND lease_token=?`,
			state,
			message,
			next,
			w.InfoHash,
			w.LeaseToken,
		).Error
	})
}

// Only used after the real block filter; exact durable tombstones remain mandatory.
type allowOne struct{}

func (allowOne) Filter(_ context.Context, h []protocol.ID) ([]protocol.ID, error) { return h, nil }

func (allowOne) Block(
	context.Context,
	[]protocol.ID,
	bool,
) error {
	return errors.New("not supported")
}
func (allowOne) Flush(context.Context) error { return nil }
func (s store) cleanup(ctx context.Context) error {
	cutoff := time.Now().Add(-s.config.StagingRetention)

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Exec(`DELETE FROM tracker_hash_observations WHERE last_seen<? AND NOT EXISTS (SELECT 1 FROM
metadata_resolution_work w WHERE w.info_hash=tracker_hash_observations.info_hash AND
w.state='leased' AND w.lease_until>now())`,
			cutoff,
		).Error; e != nil {
			return e
		}

		if e := tx.Exec(`DELETE FROM metadata_resolution_work WHERE updated_at<? AND (state<>'leased' OR
lease_until<now())`,
			cutoff,
		).Error; e != nil {
			return e
		}

		if e := tx.Exec(`UPDATE tracker_scrape_runs SET status='abandoned',finished_at=now() WHERE status='running' AND
started_at<?`,
			time.Now().Add(-2*s.config.ScrapeTimeout),
		).Error; e != nil {
			return e
		}

		return tx.Exec("DELETE FROM tracker_scrape_runs WHERE finished_at<?", cutoff).Error
	})
}
