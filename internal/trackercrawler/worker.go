package trackercrawler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/banning"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/bitmagnet-io/bitmagnet/internal/torrentwriter"
	"github.com/bitmagnet-io/bitmagnet/internal/tracker"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	errPrivate = errors.New("private metadata excluded from public tracker ingestion")
	errBusy    = errors.New("tracker announce group busy")
)

type trackerClient interface {
	Scrape(context.Context, string, tracker.Limits, func(tracker.Entry) error) (int, error)
	Announce(
		context.Context,
		string,
		protocol.ID,
		protocol.ID,
		uint16,
		string,
	) (tracker.AnnounceResult, error)
}
type crawler struct {
	store     store
	client    trackerClient
	requester metainforequester.Requester
	checker   banning.Checker
	logger    *zap.SugaredLogger
	metrics   *prometheus.CounterVec
	peerID    protocol.ID
	port      uint16
}

func (c *crawler) run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(1)

	go func() { defer wg.Done(); c.scrapes(ctx) }()

	for range c.store.config.MetadataConcurrency {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for ctx.Err() == nil {
				w, e := c.store.claim(ctx)
				if e != nil {
					c.logger.Errorw("claim failed", "error", e)
				}

				if w.LeaseToken == "" {
					if !wait(ctx, time.Second) {
						return
					}

					continue
				}

				c.resolve(ctx, w)
			}
		}()
	}

	wg.Wait()
}

func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (c *crawler) scrapes(ctx context.Context) {
	for ctx.Err() == nil {
		if e := c.store.cleanup(ctx); e != nil {
			c.logger.Errorw("tracker cleanup failed", "error", e)
		}

		for _, ep := range c.store.config.Endpoints {
			if ctx.Err() != nil {
				return
			}

			id, token, e := c.store.beginRun(ctx, ep)
			if e != nil {
				c.logger.Errorw("scrape claim failed", "tracker", ep.ID, "error", e)
				continue
			}

			if id == 0 {
				continue
			}

			scrapeCtx, scrapeCancel := context.WithTimeout(ctx, c.store.config.ScrapeTimeout)
			entries := make([]tracker.Entry, 0, 100)
			admitted := 0
			flush := func() error {
				if len(entries) == 0 {
					return nil
				}

				n, e := c.store.admit(scrapeCtx, ep, entries)
				if e == nil {
					admitted += n
					entries = entries[:0]
				}

				return e
			}
			cfg := c.store.config

			var wireBytes, decodedBytes int64

			count, runErr := c.client.Scrape(
				scrapeCtx,
				ep.ScrapeURL,
				tracker.Limits{
					OnRead:       func(wire, decoded int64) { wireBytes, decodedBytes = wire, decoded },
					WireBytes:    cfg.MaxWireBytes,
					DecodedBytes: cfg.MaxDecodedBytes,
					Hashes:       cfg.MaxHashesPerRun,
					Timeout:      cfg.ScrapeTimeout,
				},
				func(entry tracker.Entry) error {
					entries = append(entries, entry)
					if len(entries) == 100 {
						return flush()
					}

					return nil
				},
			)
			// Retain validated complete entries even when the stream is truncated. Capacity
			// failures are not immediately retried within the same scrape run.
			if !errors.Is(runErr, ErrCapacity) {
				if e := flush(); e != nil {
					runErr = errors.Join(runErr, e)
				}
			}

			scrapeCancel()

			outcome := "complete"
			if runErr != nil {
				outcome = "failed"
				if count > 0 {
					outcome = "partial"
				}
			}

			c.metrics.WithLabelValues(ep.ID, "scrape_"+outcome).Inc()
			c.metrics.WithLabelValues(ep.ID, "scrape_wire_bytes").Add(float64(wireBytes))
			c.metrics.WithLabelValues(ep.ID, "scrape_decoded_bytes").Add(float64(decodedBytes))
			c.metrics.WithLabelValues(ep.ID, "hashes_observed").Add(float64(count))
			c.metrics.WithLabelValues(ep.ID, "hashes_admitted").Add(float64(admitted))

			finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			e = c.store.endRun(
				finishCtx,
				ep,
				id,
				token,
				count,
				admitted,
				scrapeBytes{wireBytes, decodedBytes},
				runErr,
			)

			cancel()
			c.logger.Infow(
				"tracker scrape finished",
				"tracker",
				ep.ID,
				"status",
				outcome,
				"entries",
				count,
				"admitted",
				admitted,
				"error",
				errors.Join(runErr, e),
			)
		}

		if !wait(ctx, time.Minute) {
			return
		}
	}
}

func (c *crawler) resolve(parent context.Context, w work) {
	cfg := c.store.config

	ctx, cancel := context.WithTimeout(parent, cfg.ResolveTimeout)
	defer cancel()

	state := stateRetryWait

	var resultErr error

	networkAttempted := false
	remainingPeers := cfg.MaxPeersPerHash

	var input *torrentwriter.Input

	next := time.Now().Add(cfg.RetryDelay * time.Duration(1<<min(w.Attempts-1, 8)))

	origins, e := c.store.origins(ctx, w.InfoHash)
	if e != nil {
		resultErr = e
	} else {
		allowed, e := c.store.blocks.Filter(ctx, []protocol.ID{w.InfoHash})

		var t model.Torrent
		dbErr := c.store.db.WithContext(ctx).
			Where("info_hash=?", w.InfoHash).
			Find(&t).Error

		var tombstones int64

		if dbErr == nil {
			dbErr = c.store.db.WithContext(ctx).
				Table("torrent_discovery_tombstones").
				Where("info_hash=?", w.InfoHash).
				Count(&tombstones).Error
		}

		switch {
		case e != nil:
			resultErr = e
		case dbErr != nil:
			resultErr = dbErr
		case len(allowed) == 0 || t.Private || tombstones > 0:
			state = stateBlocked
		case !t.InfoHash.IsZero() && !torrentwriter.NeedsMetadata(t, c.store.files):
			state = stateResolved
		default:
			if w.Attempts > cfg.MaxResolutionAttempts {
				state = "exhausted"
				break
			}

			for _, o := range origins {
				ep, ok := c.endpoint(o.TrackerID)
				if !ok {
					continue
				}

				if remainingPeers <= 0 || ctx.Err() != nil {
					break
				}

				in, interval, e := c.fromTracker(ctx, ep, w.InfoHash, o, &remainingPeers)
				if !errors.Is(e, errBusy) {
					networkAttempted = true
				}

				if until := time.Now().Add(interval); until.After(next) {
					next = until
				}

				if errors.Is(e, errPrivate) {
					state = stateBlocked
					resultErr = e

					break
				}

				if e != nil {
					resultErr = errors.Join(resultErr, e)
					continue
				}

				input = &in
				state = stateResolved
				resultErr = nil

				break
			}

			if state != stateResolved && resultErr == nil {
				resultErr = errors.New("no eligible origin peers")
			}
		}
	}

	if state == stateRetryWait && !networkAttempted && errors.Is(resultErr, errBusy) {
		// Contention is scheduling, not a failed network attempt.
		_ = c.store.db.WithContext(ctx).
			Exec(`UPDATE metadata_resolution_work SET attempts=greatest(0,attempts-1) WHERE info_hash=? AND
lease_token=?`, w.InfoHash, w.LeaseToken).
			Error
		w.Attempts--
		next = time.Now().Add(5 * time.Second)
	}

	if state == stateRetryWait && w.Attempts >= cfg.MaxResolutionAttempts {
		state = "exhausted"
	}

	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer finishCancel()

	if e := c.store.finish(finishCtx, w, state, resultErr, input, origins, next); e != nil {
		c.logger.Errorw("resolution commit failed", "error", e)
		return
	}

	c.metrics.WithLabelValues("all", state).Inc()
}

func (c *crawler) endpoint(id string) (Endpoint, bool) {
	for _, ep := range c.store.config.Endpoints {
		if ep.ID == id {
			return ep, true
		}
	}

	return Endpoint{}, false
}

func (c *crawler) fromTracker(
	ctx context.Context,
	ep Endpoint,
	h protocol.ID,
	o observation,
	remaining *int,
) (torrentwriter.Input, time.Duration, error) {
	var empty torrentwriter.Input

	group := ep.RateLimitGroup
	if group == "" {
		u, _ := url.Parse(ep.AnnounceURL)
		group = u.Hostname()
	}

	token := protocol.RandomNodeID().String()

	acquired, e := c.store.acquireAnnounce(ctx, group, token)
	if e != nil {
		return empty, 0, e
	}

	if !acquired {
		return empty, time.Minute, errBusy
	}

	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()

		if e := c.store.releaseAnnounce(cleanup, group, token); e != nil {
			c.logger.Warnw("announce release failed", "tracker", ep.ID, "error", e)
		}
	}()
	// Even a timed-out start may have been received. Send stopped with the same session identity.
	defer func() {
		stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()

		_, e := c.client.Announce(stop, ep.AnnounceURL, h, c.peerID, c.port, "stopped")
		if e != nil {
			c.metrics.WithLabelValues(ep.ID, "stop_failed").Inc()
		}
	}()

	res, e := c.client.Announce(ctx, ep.AnnounceURL, h, c.peerID, c.port, "started")
	if e != nil {
		var he tracker.HTTPError
		if errors.As(e, &he) {
			return empty, he.RetryAfter, e
		}

		return empty, 0, e
	}

	c.metrics.WithLabelValues(ep.ID, "announce_success").Inc()

	seen := map[netip.AddrPort]bool{}
	for _, peer := range res.Peers {
		if !peer.Addr().Is4() || !tracker.PublicAddress(peer.Addr()) || seen[peer] {
			continue
		}

		seen[peer] = true

		if *remaining <= 0 {
			break
		}

		*remaining--

		mi, e := c.requester.Request(ctx, h, peer)
		if e != nil {
			continue
		}

		if mi.Info.Private != nil && *mi.Info.Private {
			return empty, res.Interval, errPrivate
		}

		if e := c.checker.Check(mi.Info); e != nil {
			return empty, res.Interval, fmt.Errorf("metadata validation: %w", e)
		}

		return torrentwriter.Input{
			Hash: h, Info: mi.Info, Source: "tracker:" + ep.ID, SourceName: "Tracker: " + ep.ID,
			Seeders: model.NewNullUint(
				uint(o.Seeders),
			), Leechers: model.NewNullUint(uint(o.Leechers)), ObservedAt: o.LastSeen,
		}, res.Interval, nil
	}

	return empty, res.Interval, errors.New("no peer returned verified metadata")
}

func (s store) acquireAnnounce(ctx context.Context, group, token string) (bool, error) {
	res := s.db.WithContext(ctx).
		Exec(`INSERT INTO tracker_announce_limits(group_id,lease_token,available_at,lease_until) VALUES (?,?,now(),?)
 ON CONFLICT(group_id) DO UPDATE SET lease_token=EXCLUDED.lease_token,lease_until=EXCLUDED.lease_until
 WHERE tracker_announce_limits.available_at<=now() AND tracker_announce_limits.lease_until<now()`,
			group, token, time.Now().Add(s.config.ResolveTimeout+15*time.Second),
		)

	return res.RowsAffected > 0, res.Error
}

func (s store) releaseAnnounce(ctx context.Context, group, token string) error {
	return s.db.WithContext(ctx).
		Exec(`UPDATE tracker_announce_limits SET lease_until=now(),available_at=now()+interval '1 second'
WHERE group_id=? AND lease_token=?`, group, token).
		Error
}

// A real ephemeral listening socket supplies the announce port. This indexer has
// no payload to serve, so incoming connections are closed without claiming pieces.
func listen(ctx context.Context, port uint16) (net.Listener, uint16, error) {
	l, e := (&net.ListenConfig{}).Listen(ctx, "tcp4", fmt.Sprintf(":%d", port))
	if e != nil {
		return nil, 0, e
	}

	go func() {
		for {
			conn, e := l.Accept()
			if e != nil {
				return
			}

			_ = conn.Close()
		}
	}()

	return l, uint16(l.Addr().(*net.TCPAddr).Port), nil
}
