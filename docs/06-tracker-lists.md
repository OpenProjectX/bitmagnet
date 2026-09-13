# Fetching public tracker lists

[Documentation index](README.md)

The [ngosang list](https://github.com/ngosang/trackerslist) and
[XIU2 collection](https://github.com/XIU2/TrackersListCollection) publish tracker
announce URLs. They do not provide torrent catalogs containing info hashes,
names, sizes, or file lists.

## What works with this checkout

Use [scripts/fetch_trackers.py](../scripts/fetch_trackers.py) to download both
projects' complete lists and write a merged file:

```bash
python3 scripts/fetch_trackers.py
```

The default output is `/tmp/bitmagnet-trackers.txt`. Python 3.8 or newer is
required; there are no third-party dependencies. To choose a persistent location
and exclude WebSocket trackers:

```bash
python3 scripts/fetch_trackers.py \
  --output "$HOME/.local/share/trackers.txt" \
  --schemes udp http https
```

The script fetches both sources over verified HTTPS, validates entries, removes
exact duplicate URLs, preserves source order, and writes blank-line-separated
URLs suitable for clients that accept that format. It preserves paths, queries,
and different endpoints on the same hostname. It does not resolve hostnames or
test whether each tracker is reachable from your network.

Both sources must succeed and contain valid entries before the output is
replaced. A timeout, invalid response, or empty filtered result exits nonzero
and leaves any existing output unchanged. Fetching is bounded to 2 MiB per
source. Re-running refreshes the file rather than appending stale URLs.

Import this file using the tracker settings of a compatible BitTorrent client.
The script does not configure such a client automatically.

## Why it cannot import these URLs into Bitmagnet

The supplied instance at
`https://bitmagnet.cn-guangzhou-a.k8s.openprojectx.org/` was reachable during
inspection on 2026-09-13. A read-only GraphQL introspection query returned
`torrent` and `queue` as its root mutation fields. The local source defines no
tracker-list management or tracker discovery implementation. This check is not
a claim that every behavior of the deployed binary was audited.

The [import item](../internal/importer/importer.go) describes torrent records,
including `InfoHash`, `Name`, `Size`, and `Source`. The
[`POST /import` handler](../internal/importer/httpserver/httpserver.go) decodes
newline-separated JSON records of that type. A tracker URL cannot supply those
missing torrent fields. Creating a source named after a tracker would not
implement tracker discovery either.

Consequently, the script has no Bitmagnet import flag and makes no requests to
the instance. Tracker URLs also must not be copied into
`dht_crawler.bootstrap_nodes`: a UDP tracker endpoint and a DHT endpoint speak
different protocols even when their hostnames or ports happen to match.

## What would be needed to grow the catalog

For immediate catalog ingestion, use an actual torrent dataset or an indexer's
export and convert its records to the format in the
[existing import guide](../bitmagnet.io/guides/import.md).

Tracker-assisted metadata acquisition would require additional implementation:
given a known info hash, query a relevant tracker for peers, retrieve and verify
metadata from those peers, then import the torrent record. Tracker announce
requests require an info hash; adding a tracker list alone does not implement
enumeration of unknown torrents. See [BEP 3](https://www.bittorrent.org/beps/bep_0003.html)
and [BEP 15](https://www.bittorrent.org/beps/bep_0015.html).

That feature would be a separate network client or backend extension, beyond
fetching a text list. No tracker URLs or placeholder torrent records were
submitted to the deployed Bitmagnet instance by this task.

## Full scrape: an exception for discovering unknown hashes

Some HTTP trackers expose a **full scrape**: a scrape request without an
`info_hash` parameter returns hash keys and swarm statistics. Opentracker
implements this feature and allows operators to disable it. Thus the claim
that trackers can *never* supply unknown hashes is incorrect. Support depends
on the tracker implementation and deployment; see the
[opentracker documentation](https://erdgeist.org/arts/software/opentracker/).

A normal scrape queries specified hashes. Its response maps binary 20-byte
hashes to counters such as `complete`, `incomplete`, and `downloaded`. These are
**swarm statistics**, not the torrent info dictionary containing names, file
paths, lengths, and payload piece hashes. See
[BEP 48](https://www.bittorrent.org/beps/bep_0048.html).

| Mechanism | Requires a known torrent hash? | Returns |
| --- | --- | --- |
| DHT `sample_infohashes` (BEP 51) | No | A sample of stored hashes and routing contacts |
| Tracker announce | Yes | Peer contacts |
| Ordinary tracker scrape | Yes | Statistics for supplied hashes |
| HTTP full scrape, when exposed | No | Tracked hash keys and statistics |
| Peer `ut_metadata` exchange | Yes | The torrent info dictionary |

The current Bitmagnet crawler gets new hashes in
[`runSampleInfoHashes`](../internal/dhtcrawler/sample_infohashes.go), deduplicates
them, and asks the sampling node for peers. It obtains the info dictionary from
those peers. Neither hash sampling nor full scrape supplies that dictionary.
DHT nodes learn swarm hashes through peer announcements; Bitmagnet is not
brute-forcing the 160-bit hash space.

A separate tracker-based ingestion pipeline could perform:

```text
HTTP full scrape -> unknown hashes -> tracker/DHT peer lookup
                 -> peer metadata retrieval -> hash verification
                 -> torrent records -> Bitmagnet /import
```

This checkout does not implement that pipeline. A failed or disabled full
scrape does not mean the tracker cannot find peers for an already-known hash.
Likewise, an empty `files` dictionary does not demonstrate that a server can
actually enumerate a nonempty catalog.

## Reproduce the capability survey

```bash
python3 scripts/fetch_trackers.py --output /tmp/trackers-survey.txt
python3 scripts/probe_tracker_scrapes.py /tmp/trackers-survey.txt \
  --output /tmp/tracker-scrape-survey.json
```

The [probe script](../scripts/probe_tracker_scrapes.py) maps `announce` to
`scrape` in each listed HTTP(S) URL's path and sends a GET without supplying a
hash. It does not invent HTTP endpoints for UDP or WebSocket URLs. Standard UDP
scrape requires supplied hashes; UDP-only entries are outside this full-scrape
probe's scope, not evidence of nonfunctional trackers.

The probe uses six workers, a six-second socket timeout, a checked twelve-second
read budget, and limits of 256 KiB received and 1 MiB decoded per endpoint.
The read budget is checked between reads, so an individual read or DNS lookup
can extend elapsed time. Gzip is decoded within the output limit. TLS
verification stays enabled, and normal environment proxy settings apply.

A positive result requires a bencoded `files` dictionary with a 20-byte hash key
and nonnegative integer scrape counters. HTTP 200 by itself is insufficient.
The probe stops as soon as one valid entry appears; it does **not** download
whole catalogs or prove that a server returns every hash it tracks. Results
retain endpoints, status, byte counts, timestamps, and errors, but no peer IPs
or torrent metadata. There are no automatic retries within a run.

See the [2026-09-13 survey report](reports/tracker-scrape-survey-2026-09-13.md)
for observed counts, limitations, and machine-readable evidence. No announces,
payload downloads, or Bitmagnet imports are performed by the survey.
