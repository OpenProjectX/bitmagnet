package trackercrawler

import (
	"reflect"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/config/configresolver"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/require"
)

func TestConfiguration(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	require.NoError(t, c.Validate())
	c.Enabled = true
	require.Error(t, c.Validate())
	c.Endpoints = []Endpoint{{ID: "fixture", AnnounceURL: "https://example.org/announce"}}
	require.NoError(t, c.Validate())
	c.Endpoints = append(c.Endpoints, c.Endpoints[0])
	require.Error(t, c.Validate())
	c.Endpoints = c.Endpoints[:1]
	c.MaxPendingHashes = 0
	require.Error(t, c.Validate())
	c.Enabled = false
	c.StagingRetention = 0
	require.Error(t, c.Validate())

	r := configresolver.NewMap(
		map[string]interface{}{
			"tracker_crawler": map[string]interface{}{
				"endpoints": []interface{}{
					map[string]interface{}{
						"id":           "fixture",
						"announce_url": "https://example.org/announce",
					},
				},
			},
		},
		validator.New(),
	)
	value, ok, e := r.Resolve(
		[]string{"tracker_crawler", "endpoints"},
		reflect.TypeOf([]Endpoint{}),
	)
	require.NoError(t, e)
	require.True(t, ok)
	require.NotNil(t, value)
}
