package trackercrawlerfx

import (
	"github.com/bitmagnet-io/bitmagnet/internal/config/configfx"
	"github.com/bitmagnet-io/bitmagnet/internal/trackercrawler"
	"go.uber.org/fx"
)

func New() fx.Option {
	return fx.Module(
		"tracker_crawler",
		configfx.NewConfigModule[trackercrawler.Config](
			"tracker_crawler",
			trackercrawler.NewDefaultConfig(),
		),
		fx.Provide(trackercrawler.New),
	)
}
