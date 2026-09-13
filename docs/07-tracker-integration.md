# Native tracker-assisted discovery

[Documentation index](README.md) · [RFC 0001](rfcs/0001-tracker-assisted-discovery.md)

The HTTP(S)-first implementation adds an optional `tracker_crawler` worker:

```text
explicit tracker endpoints -> bounded full scrape -> deduplicated hash work
 -> originating tracker announce -> peer ut_metadata -> verified metadata
 -> torrent/files/source/job transaction -> classification and search
```

It remains **disabled by default**, including with `worker run --all`. It does
not automatically import either public tracker list, download payload files,
perform UDP tracker announces, or run iterative DHT fallback. The existing
`/import` format and known-hash API behavior are unchanged.

## Enable a small pilot

Build a binary from this checkout. Select a public HTTP(S) tracker that exposes
full scrape; the endpoint below is a placeholder, not a service to contact:

```yaml
tracker_crawler:
  enabled: true
  endpoints:
    - id: selected-tracker
      announce_url: https://tracker.example.org/announce
      scrape_url: https://tracker.example.org/scrape
      rate_limit_group: selected-operator
  metadata_concurrency: 2
  max_hashes_per_run: 1000
  max_pending_hashes: 1000
```

Place this in the application's `config.yml` and run:

```bash
bitmagnet worker run --keys=http_server --keys=queue_server --keys=dht_crawler --keys=tracker_crawler
```

`worker run --all` also includes the new worker. `config show` displays effective
values; configuration is validated before worker startup. No public known-hash
resolver endpoint is added. An old upstream image does not contain this feature.

Endpoint IDs must be unique lowercase alphanumeric identifiers with optional
hyphens/underscores, up to 64 characters. URLs must use HTTP(S), with no embedded
credentials or fragments. Omit `scrape_url` only when replacing `announce` in
the path gives the correct scrape URL. Do not include `info_hash` in a full-scrape
URL. Alias endpoints can share an explicit `rate_limit_group`; otherwise the
announce hostname is the group. No more than 64 endpoints can be configured.

## Limits and scheduling

| Config key under `tracker_crawler` | Default | Purpose                                                     |
| ---------------------------------- | ------- | ----------------------------------------------------------- |
| `full_scrape_interval`             | `24h`   | Interval before the next run; minimum one hour              |
| `scrape_timeout`                   | `2m`    | Deadline for the scrape and its admission callbacks         |
| `max_wire_bytes`                   | 8 MiB   | Maximum compressed/wire body bytes                          |
| `max_decoded_bytes`                | 64 MiB  | Maximum decompressed bytes                                  |
| `max_hashes_per_run`               | 100,000 | Maximum parsed entries                                      |
| `max_pending_hashes`               | 10,000  | Global pending/leased/retry admission bound                 |
| `metadata_concurrency`             | 16      | Maximum resolver loops per process                          |
| `max_peers_per_hash`               | 8       | Maximum peer attempts across all origins for one hash       |
| `resolve_timeout`                  | `1m`    | Deadline for a hash's tracker/metadata work                 |
| `max_resolution_attempts`          | 3       | Attempt budget, including initial attempt                   |
| `retry_delay`                      | `1h`    | Exponential base delay: one hour, two hours, etc.           |
| `staging_retention`                | `168h`  | Expiration for staging/work/run history                     |
| `peer_port`                        | 0       | Actual local peer listener; 0 selects an ephemeral TCP port |

Scrapes are sequential within a process, and database endpoint leases prevent
two processes scraping the same configured endpoint concurrently. Different
processes may scrape different endpoints at the same time. Resolver limits are
per process; staging bounds and tracker announce-group leases are shared through
PostgreSQL. Keep one crawler replica for the initial pilot.

Each announce group permits one active session, followed by a one-second gap.
An individual hash's retry is delayed at least as long as the returned tracker
interval. HTTP `Retry-After` can extend scheduling delays. Group contention is
rescheduled without consuming the network-attempt budget. Terminal resolution
work stays deduplicated until retention cleanup makes it eligible for later
rediscovery. Observations are capped at five times `max_pending_hashes` globally.

A hash cap, malformed/truncated stream, response byte cap, or deadline makes the
run partial/failed. Already validated batches may remain staged. There is no
pagination or resumable cursor: repeatedly truncating a sorted large catalog can
miss entries near the end. Run history makes partial coverage explicit.

## Metadata-only tracker sessions

The announce uses `started`, zero uploaded/downloaded payload bytes, and
`left=-1` while payload length is unknown. The latter follows
`bytesLeftAnnounce` in the pinned anacrolix/torrent v1.58.0 dependency. This is a
client compatibility convention, not a guarantee that every tracker accepts it.
The same session ID and actual listening port are used for a bounded `stopped`
attempt when leaving the session. No `completed` event is sent.

The listener immediately closes inbound connections because the indexer serves
no payload. It is not an added seeding service, and Kubernetes does not need to
publish it for the outbound metadata workflow. NAT/reachability and remote
tracker policies can still affect the peers returned.

Peer retrieval uses IPv4 TCP, requires the extension handshake, requests only
`ut_metadata`, and checks the resulting SHA-1 against the requested hash. The
requester now validates block bounds, handles duplicate/out-of-order blocks,
checks semantic v1 lengths, reports handshake write failures, and closes sockets
on cancellation. Private metadata is excluded from this public tracker path.

## Persistence and deletion semantics

Both DHT and tracker ingestion use the shared `internal/torrentwriter` service.
It retains full file counts and the first N file entries according to the
existing DHT `save_files_threshold`, and honors `dht_crawler.save_pieces`.
It creates classification jobs only for new or materially enriched metadata.
Repeated tracker statistics update source and derived search counts without
reclassifying unchanged metadata. Source keys are `tracker:<id>`, never `dht`.
Displayed counts keep the existing maximum-across-sources policy, not a sum.

Migration 21 adds endpoint/run/observation/work tables and announce-group leases.
Network work happens outside DB transactions; lease tokens reject stale results.
Metadata, files, source, and classification jobs commit together. An expired
lease can be reclaimed, and retry after an interrupted completion does not
create a duplicate torrent/classification job.

**Deletion behavior changes:** exact `torrent_discovery_tombstones` prevent
automatic DHT or tracker rediscovery of hashes deleted after this migration.
Deletion triggers and ingestion use the same advisory transaction lock. Blocking
also persists markers for hashes that do not yet have torrent rows and now
flushes before returning. This closes races caused by buffered blocks or a late
network response.

These markers do not expire automatically; removing them would permit deleted
hashes to return. Their storage grows with explicit deletions/blocks. If an
operator intentionally wants automatic rediscovery again, the corresponding
marker must be removed, and any existing block filter may still exclude the
hash. There is no new unblock UI/API in this phase. Explicit `/import` retains
its existing behavior, but automatic ingestion still checks the marker.

The advisory lock covers short database writes, never tracker or peer requests.
It serializes discovery writes and deletes across processes; measure its cost
on high-volume deployments before raising concurrency.

## Inspect and clean up

Read-only SQL examples:

```sql
SELECT id, capability, next_run, failures, last_error FROM tracker_endpoints;
SELECT tracker_id, status, entries, admitted, error
FROM tracker_scrape_runs ORDER BY id DESC LIMIT 20;
SELECT state, count(*) FROM metadata_resolution_work GROUP BY state;
SELECT source, count(*) FROM torrents_torrent_sources
WHERE source LIKE 'tracker:%' GROUP BY source;
```

The crawler periodically expires staging data. Cleanup also works without
running network workers:

```bash
bitmagnet tracker cleanup
```

Disabling the worker stops new claims; in-flight work is canceled and cleanup
attempts have bounded deadlines. Valid catalog records remain. Changing endpoint
selection prevents new claims for hashes whose only origins are no longer
configured; retention cleanup eventually removes their staging rows.

Prometheus counter `bitmagnet_tracker_crawler_events_total{tracker,outcome}`
reports scrape completion/partial/failure, observed/admitted hashes, announce
success, stopped-event failure, and resolution outcomes. `tracker="all"` denotes
aggregate resolution outcomes. These counters are not a count of unique new
searchable torrents; use source records, job status, and search visibility when
measuring incremental yield.

## Network and Helm

Tracker requests use **direct HTTP(S) egress** with TLS verification and resolved
address validation. They intentionally do not inherit the global TMDB HTTP proxy:
a generic proxy could bypass destination checks. This is a documented deviation
from the RFC's survey-script proxy behavior. Private/loopback/link-local and
special-use destinations are rejected; this public tracker feature has no
internal-tracker override. Redirect destinations undergo the same dial checks.
Metadata peers must also be public IPv4 addresses.

The local chart at `/data/Git/charts/bitmagnet` has configuration support in
version 0.1.2. `trackerCrawler` chart values are mounted under `tracker_crawler`
in `/config/bitmagnet/config.yml`, with a checksum to restart pods on changes.
The infrastructure Helmfile forwards `bitmagnet.trackerCrawler` while preserving
its proxy settings. The published-chart pin, production image, and environment
remain unchanged: build/publish a tracker-enabled image and the chart before
selecting them for a deployment. No cluster has been enabled by this change.

## Tests and remaining evaluation

Protocol tests use local HTTP and TCP fixtures; the metadata-only fixture checks
that no payload request follows retrieval. PostgreSQL tests exercise migrations,
atomic rollback, deduplication, lease reclaim, stale completion, deletion markers,
source races, and the resolver orchestrator. Run:

```bash
go test ./internal/tracker ./internal/protocol/metainfo/...
BITMAGNET_TEST_POSTGRES='host=127.0.0.1 port=5432 user=postgres dbname=bitmagnet_test sslmode=disable' \
  go test -race ./internal/trackercrawler -run TestPostgresIntegration -v
```

Use a disposable PostgreSQL database with permission to create schemas/extensions;
the integration test creates and drops its own schema. It skips when the
connection variable is absent. Public trackers are not CI dependencies.

The live full-scrape survey predates this implementation. A production-network
pilot, unique-yield comparison, real-world announce compatibility measurements,
and seven-day canary still need to be performed. UDP announces, iterative DHT
fallback, and an authenticated known-hash resolver API remain deferred RFC phases.

### Scrape byte accounting

Run history stores `wire_bytes` and `decoded_bytes` for consumed response bodies.
The event counter also exposes outcomes `scrape_wire_bytes` and
`scrape_decoded_bytes`. These exclude HTTP headers, failed response bodies before
parsing starts, announce traffic, and peer metadata traffic; they are not total
network utilization. CI runs the PostgreSQL integration suite against PostgreSQL 16.
