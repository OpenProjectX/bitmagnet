package trackercrawler

import (
	"context"
	"net"
	"sync"

	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/dhtcrawler"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/banning"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/bitmagnet-io/bitmagnet/internal/tracker"
	"github.com/bitmagnet-io/bitmagnet/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Params struct {
	fx.In
	Config    Config
	DHTConfig dhtcrawler.Config
	DB        lazy.Lazy[*gorm.DB]
	Blocks    lazy.Lazy[blocking.Manager]
	Requester metainforequester.Requester
	Checker   banning.Checker `name:"metainfo_banning_checker"`
	Logger    *zap.SugaredLogger
	DBWait    *sync.WaitGroup `name:"pgx_pool_wait"`
}
type Result struct {
	fx.Out
	Worker  worker.Worker        `group:"workers"`
	Metrics prometheus.Collector `group:"prometheus_collectors"`
	Command *cli.Command         `group:"commands"`
}

func New(p Params) (Result, error) {
	if e := p.Config.Validate(); e != nil {
		return Result{}, e
	}

	for i := range p.Config.Endpoints {
		if p.Config.Endpoints[i].ScrapeURL == "" {
			p.Config.Endpoints[i].ScrapeURL, _ = tracker.ScrapeURL(
				p.Config.Endpoints[i].AnnounceURL,
			)
		}
	}

	metric := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "tracker_crawler",
			Name:      "events_total",
			Help:      "Tracker discovery and resolution outcomes.",
		},
		[]string{"tracker", "outcome"},
	)
	makeStore := func() (store, error) {
		db, e := p.DB.Get()
		if e != nil {
			return store{}, e
		}

		blocks, e := p.Blocks.Get()

		return store{
			db:     db,
			blocks: blocks,
			config: p.Config,
			files:  p.DHTConfig.SaveFilesThreshold,
			pieces: p.DHTConfig.SavePieces,
		}, e
	}

	var cancel context.CancelFunc

	var done chan struct{}

	var listener net.Listener

	w := worker.NewWorker("tracker_crawler", fx.Hook{
		OnStart: func(startCtx context.Context) error {
			if !p.Config.Enabled {
				return nil
			}
			s, e := makeStore()
			if e != nil {
				return e
			}
			ctx, stop := context.WithCancel(context.WithoutCancel(startCtx))
			cancel = stop
			if e = s.sync(ctx); e != nil {
				cancel()
				return e
			}
			var port uint16
			listener, port, e = listen(ctx, p.Config.PeerPort)
			if e != nil {
				cancel()
				return e
			}
			client := tracker.NewClient()
			done = make(chan struct{})
			p.DBWait.Add(1)
			c := crawler{
				store:     s,
				client:    client,
				requester: p.Requester,
				checker:   p.Checker,
				logger:    p.Logger.Named("tracker_crawler"),
				metrics:   metric,
				peerID:    protocol.RandomPeerID(),
				port:      port,
			}
			go func() { defer close(done); defer p.DBWait.Done(); defer client.Close(); c.run(ctx) }()
			return nil
		}, OnStop: func(ctx context.Context) error {
			if cancel == nil {
				return nil
			}
			cancel()
			if listener != nil {
				_ = listener.Close()
			}
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})

	return Result{
		Worker:  w,
		Metrics: metric,
		Command: &cli.Command{
			Name: "tracker",
			Subcommands: []*cli.Command{
				{
					Name:  "cleanup",
					Usage: "Remove expired tracker staging data, including while crawling is disabled",
					Action: func(ctx *cli.Context) error {
						s, e := makeStore()
						if e != nil {
							return e
						}
						return s.cleanup(ctx.Context)
					},
				},
			},
		},
	}, nil
}
