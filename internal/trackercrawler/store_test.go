package trackercrawler

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	ami "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/banning"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/bitmagnet-io/bitmagnet/internal/torrentwriter"
	"github.com/bitmagnet-io/bitmagnet/internal/tracker"
	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
	"github.com/pressly/goose/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestPostgresIntegration(t *testing.T) {
	t.Parallel()

	dsn := os.Getenv("BITMAGNET_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set BITMAGNET_TEST_POSTGRES to a disposable PostgreSQL database")
	}

	ctx := context.Background()
	admin, e := gorm.Open(
		postgres.Open(dsn),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
	)
	require.NoError(t, e)

	schema := fmt.Sprintf("tracker_test_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() {
		_ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error
		db, _ := admin.DB()
		_ = db.Close()
	})

	db, e := gorm.Open(
		postgres.Open(dsn+" search_path="+schema+",public"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
	)
	require.NoError(t, e)
	sqlDB, e := db.DB()
	require.NoError(t, e)

	defer sqlDB.Close()
	provider, e := goose.NewProvider(goose.DialectPostgres, sqlDB, migrationssql.FS)
	require.NoError(t, e)
	_, e = provider.Up(ctx)
	require.NoError(t, e)

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	cfg.Endpoints = []Endpoint{
		{
			ID:          "fixture",
			AnnounceURL: "https://example.org/announce",
			ScrapeURL:   "https://example.org/scrape",
		},
	}
	s := store{db: db, blocks: allowOne{}, config: cfg, files: 2, pieces: true}
	require.NoError(t, s.sync(ctx))

	makeInput := func(name string) torrentwriter.Input {
		info := ami.Info{
			Name:        name,
			PieceLength: 16384,
			Pieces:      make([]byte, 20),
			Files: []ami.FileInfo{
				{Length: 1024, Path: []string{"a.txt"}},
				{Length: 2048, Path: []string{"b.txt"}},
				{Length: 4096, Path: []string{"c.txt"}},
			},
		}
		raw, e := bencode.Marshal(info)
		require.NoError(t, e)

		return torrentwriter.Input{
			Hash:       protocol.ID(ami.HashBytes(raw)),
			Info:       info,
			Source:     "tracker:fixture",
			SourceName: "fixture",
			ObservedAt: time.Now(),
			Seeders:    model.NewNullUint(5),
			Leechers:   model.NewNullUint(2),
		}
	}
	count := func(table string) int64 { var n int64; require.NoError(t, db.Table(table).Count(&n).Error); return n }
	input := makeInput("first-fixture")
	entry := tracker.Entry{Hash: input.Hash, Seeders: 5, Leechers: 2, Downloaded: 6}
	n, e := s.admit(ctx, cfg.Endpoints[0], []tracker.Entry{entry, entry})
	require.NoError(t, e)
	require.Equal(t, 1, n)
	n, e = s.admit(ctx, cfg.Endpoints[0], []tracker.Entry{entry})
	require.NoError(t, e)
	require.Zero(t, n)

	w, e := s.claim(ctx)
	require.NoError(t, e)
	require.NotEmpty(t, w.LeaseToken)

	other, e := s.claim(ctx)
	require.NoError(t, e)
	require.Empty(t, other.LeaseToken)
	// Expiration permits reclaim; stale owners cannot complete.
	require.NoError(
		t,
		db.Exec(
			`UPDATE metadata_resolution_work SET lease_until=now()-interval '1 second' WHERE info_hash=?`,
			input.Hash,
		).Error,
	)

	renewed, e := s.claim(ctx)
	require.NoError(t, e)
	require.NotEqual(t, w.LeaseToken, renewed.LeaseToken)
	require.ErrorIs(t, s.finish(ctx, w, "resolved", nil, &input, nil, time.Now()), ErrLease)
	origins, e := s.origins(ctx, input.Hash)
	require.NoError(t, e)
	require.NoError(t, s.finish(ctx, renewed, "resolved", nil, &input, origins, time.Now()))
	require.EqualValues(t, 1, count("torrents"))
	require.EqualValues(t, 2, count("torrent_files"))
	require.EqualValues(t, 1, count("torrent_pieces"))
	require.EqualValues(t, 1, count("queue_jobs"))

	var stored model.Torrent

	require.NoError(t, db.Where("info_hash=?", input.Hash).Take(&stored).Error)
	require.Equal(t, model.FilesStatusOverThreshold, stored.FilesStatus)
	require.EqualValues(t, 3, stored.FilesCount.Uint)
	// Duplicate metadata and a source change do not requeue or downgrade retained files.
	duplicate := input
	duplicate.Source = "dht"
	duplicate.SourceName = "DHT"
	_, e = torrentwriter.Write(
		ctx,
		db,
		allowOne{},
		[]torrentwriter.Input{duplicate},
		torrentwriter.Options{SaveFilesThreshold: 1},
	)
	require.NoError(t, e)
	require.EqualValues(t, 1, count("queue_jobs"))
	require.EqualValues(t, 2, count("torrent_files"))
	// A source observation for a complete/over-threshold record does not schedule new metadata work.
	_, e = s.admit(ctx, cfg.Endpoints[0], []tracker.Entry{entry})
	require.NoError(t, e)
	// Deletion tombstones protect both discovery paths, including a stale verified result.
	require.NoError(t, db.Exec("DELETE FROM torrents WHERE info_hash=?", input.Hash).Error)
	hashes, e := torrentwriter.Write(
		ctx,
		db,
		allowOne{},
		[]torrentwriter.Input{input},
		torrentwriter.Options{SaveFilesThreshold: 2},
	)
	require.NoError(t, e)
	require.Empty(t, hashes)
	require.EqualValues(t, 0, count("torrents"))
	// Simulate failure in the final queue insert: all preceding metadata rows roll back.
	require.NoError(
		t,
		db.Exec(
			`CREATE FUNCTION fail_queue() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'fixture failure'; END $$;
 CREATE TRIGGER fail_queue BEFORE INSERT ON queue_jobs FOR EACH ROW EXECUTE FUNCTION fail_queue();`,
		).Error,
	)

	second := makeInput("second-fixture")
	_, e = torrentwriter.Write(
		ctx,
		db,
		allowOne{},
		[]torrentwriter.Input{second},
		torrentwriter.Options{SaveFilesThreshold: 2},
	)
	require.Error(t, e)
	require.Contains(t, e.Error(), "fixture failure")
	require.EqualValues(t, 0, count("torrents"))
	require.NoError(
		t,
		db.Exec("DROP TRIGGER fail_queue ON queue_jobs; DROP FUNCTION fail_queue()").Error,
	)
	// Commit followed by crash before work completion: retry must not duplicate classification.
	_, e = s.admit(ctx, cfg.Endpoints[0], []tracker.Entry{{Hash: second.Hash}})
	require.NoError(t, e)
	secondWork, e := s.claim(ctx)
	require.NoError(t, e)
	require.Equal(t, second.Hash, secondWork.InfoHash)
	_, e = torrentwriter.Write(
		ctx,
		db,
		allowOne{},
		[]torrentwriter.Input{second},
		torrentwriter.Options{SaveFilesThreshold: 2},
	)
	require.NoError(t, e)

	jobs := count("queue_jobs")

	require.NoError(t, s.finish(ctx, secondWork, "resolved", nil, &second, nil, time.Now()))
	require.Equal(t, jobs, count("queue_jobs"))
	// Shared announce leases prevent parallel requests and maintain the release gap.
	ok, e := s.acquireAnnounce(ctx, "fixture", "one")
	require.NoError(t, e)
	require.True(t, ok)
	ok, e = s.acquireAnnounce(ctx, "fixture", "two")
	require.NoError(t, e)
	require.False(t, ok)
	require.NoError(t, s.releaseAnnounce(ctx, "fixture", "one"))
	ok, e = s.acquireAnnounce(ctx, "fixture", "two")
	require.NoError(t, e)
	require.False(t, ok)
	// Capacity is transactional, not approximate admission beyond the configured bound.
	s.config.MaxPendingHashes = 1
	third := makeInput("third-fixture")
	_, e = s.admit(ctx, cfg.Endpoints[0], []tracker.Entry{{Hash: third.Hash}})
	require.NoError(t, e)

	fourth := makeInput("fourth-fixture")
	_, e = s.admit(ctx, cfg.Endpoints[0], []tracker.Entry{{Hash: fourth.Hash}})
	require.ErrorIs(t, e, ErrCapacity)
	require.NoError(t, s.cleanup(ctx))
	// Exercise the real resolution orchestrator with deterministic tracker and metadata adapters.
	pending, e := s.claim(ctx)
	require.NoError(t, e)
	require.Equal(t, third.Hash, pending.InfoHash)

	fake := &fixtureTracker{}
	runner := crawler{
		store: s, client: fake, requester: fixtureMetadata{third.Info}, checker: banning.New(banning.Params{}).Checker,
		logger: zap.NewNop().
			Sugar(),
		metrics: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "test_tracker_events"},
			[]string{"tracker", "outcome"},
		), peerID: protocol.RandomPeerID(), port: 12345,
	}
	runner.resolve(ctx, pending)

	var state string

	require.NoError(
		t,
		db.Raw("SELECT state FROM metadata_resolution_work WHERE info_hash=?", third.Hash).
			Scan(&state).
			Error,
	)
	require.Equal(t, "resolved", state)
	require.Equal(t, []string{"started", "stopped"}, fake.events)
	require.EqualValues(t, 2, count("torrents"))
	// Parallel sources must create one torrent and one classification job.
	fifth := makeInput("fifth-fixture")
	jobs = count("queue_jobs")

	var wg sync.WaitGroup

	errs := make(chan error, 2)

	for _, source := range []string{"dht", "tracker:fixture"} {
		wg.Add(1)

		go func(source string) {
			defer wg.Done()

			in := fifth
			in.Source = source
			writerDB := db
			if source == "dht" {
				// Match the production DHT call: this handle carries a Torrent model/table.
				writerDB = dao.Use(db).Torrent.WithContext(ctx).UnderlyingDB()
			}
			_, e := torrentwriter.Write(
				ctx,
				writerDB,
				allowOne{},
				[]torrentwriter.Input{in},
				torrentwriter.Options{SaveFilesThreshold: 2},
			)
			errs <- e
		}(source)
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		require.NoError(t, e)
	}

	require.Equal(t, jobs+1, count("queue_jobs"))
	require.EqualValues(t, 3, count("torrents"))
	// Additive migration can be rolled back after old data remains.
	_, e = provider.Down(ctx)
	require.NoError(t, e)
	require.EqualValues(t, 3, count("torrents"))
}

type fixtureTracker struct{ events []string }

func (*fixtureTracker) Scrape(
	context.Context,
	string,
	tracker.Limits,
	func(tracker.Entry) error,
) (int, error) {
	return 0, nil
}

func (f *fixtureTracker) Announce(
	_ context.Context,
	_ string,
	_ protocol.ID,
	_ protocol.ID,
	_ uint16,
	event string,
) (tracker.AnnounceResult, error) {
	f.events = append(f.events, event)

	return tracker.AnnounceResult{
		Peers:    []netip.AddrPort{netip.MustParseAddrPort("8.8.8.8:6881")},
		Interval: time.Minute,
	}, nil
}

type fixtureMetadata struct{ info ami.Info }

func (f fixtureMetadata) Request(
	context.Context,
	protocol.ID,
	netip.AddrPort,
) (metainforequester.Response, error) {
	return metainforequester.Response{Info: f.info}, nil
}
