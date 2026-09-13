package trackercrawler

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/tracker"
)

type Endpoint struct {
	ID             string
	AnnounceURL    string
	ScrapeURL      string
	RateLimitGroup string
}
type Config struct {
	Enabled               bool
	Endpoints             []Endpoint
	FullScrapeInterval    time.Duration
	ScrapeTimeout         time.Duration
	MaxWireBytes          int64
	MaxDecodedBytes       int64
	MaxHashesPerRun       int
	MaxPendingHashes      int
	MetadataConcurrency   int
	MaxPeersPerHash       int
	ResolveTimeout        time.Duration
	MaxResolutionAttempts int
	RetryDelay            time.Duration
	StagingRetention      time.Duration
	PeerPort              uint16
}

func NewDefaultConfig() Config {
	return Config{
		FullScrapeInterval: 24 * time.Hour,
		ScrapeTimeout:      2 * time.Minute,
		MaxWireBytes:       8 << 20, MaxDecodedBytes: 64 << 20,
		MaxHashesPerRun: 100000, MaxPendingHashes: 10000, MetadataConcurrency: 16, MaxPeersPerHash: 8,
		ResolveTimeout:        time.Minute,
		MaxResolutionAttempts: 3, RetryDelay: time.Hour, StagingRetention: 7 * 24 * time.Hour,
	}
}

func (c Config) Validate() error {
	if (c.Enabled && len(c.Endpoints) == 0) || len(c.Endpoints) > 64 {
		return errors.New("tracker_crawler requires 1..64 explicit endpoints")
	}

	if c.FullScrapeInterval < time.Hour || c.ScrapeTimeout <= 0 ||
		c.ScrapeTimeout > 10*time.Minute ||
		c.ResolveTimeout < time.Second ||
		c.ResolveTimeout > 5*time.Minute {
		return errors.New("invalid tracker timing limits")
	}

	if c.MaxWireBytes < 1 || c.MaxWireBytes > 64<<20 || c.MaxDecodedBytes < 1 ||
		c.MaxDecodedBytes > 256<<20 ||
		c.MaxHashesPerRun < 1 ||
		c.MaxHashesPerRun > 1000000 ||
		c.MaxPendingHashes < 1 ||
		c.MaxPendingHashes > 1000000 {
		return errors.New("invalid tracker storage limits")
	}

	if c.MetadataConcurrency < 1 || c.MetadataConcurrency > 64 || c.MaxPeersPerHash < 1 ||
		c.MaxPeersPerHash > 50 ||
		c.MaxResolutionAttempts < 1 ||
		c.MaxResolutionAttempts > 10 ||
		c.RetryDelay < time.Minute ||
		c.StagingRetention < time.Hour {
		return errors.New("invalid tracker resolution limits")
	}

	seen := map[string]bool{}
	urls := map[string]bool{}

	for _, ep := range c.Endpoints {
		if !regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`).MatchString(ep.ID) || seen[ep.ID] {
			return fmt.Errorf("invalid or duplicate tracker ID %q", ep.ID)
		}

		seen[ep.ID] = true

		if _, e := tracker.ValidateURL(ep.AnnounceURL); e != nil {
			return e
		}

		if urls[ep.AnnounceURL] {
			return errors.New("duplicate tracker announce URL")
		}

		urls[ep.AnnounceURL] = true

		if ep.ScrapeURL != "" {
			u, e := tracker.ValidateURL(ep.ScrapeURL)
			if e != nil {
				return e
			}

			if _, ok := u.Query()["info_hash"]; ok {
				return errors.New("full scrape URL cannot include info_hash")
			}
		} else if _, e := tracker.ScrapeURL(ep.AnnounceURL); e != nil {
			return e
		}
	}

	return nil
}
