# Hands-on study and troubleshooting

[Documentation index](README.md) · Previous: [Application and storage](04-application-and-storage.md)

The first exercises are offline. Later commands are optional examples for a configured local instance. They were not used to start a crawler or deployment while writing these documents.

## 1. Read and hash the bundled torrent offline

The repository contains an [Ubuntu torrent fixture](../internal/protocol/metainfo/examples/ubuntu-23.04-desktop-amd64.iso.torrent). Its [test](../internal/protocol/metainfo/read_torrent_file_test.go) expects:

| Field | Value |
| --- | --- |
| Name | `ubuntu-23.04-desktop-amd64.iso` |
| Payload size | 4,932,407,296 bytes |
| Payload piece length | 262,144 bytes |
| Primary tracker | `https://torrent.ubuntu.com/announce` |

From the repository root, run the existing parser test:

```bash
go test ./internal/protocol/metainfo -run TestReadTorrentFile -v
```

The test reads local bytes and does not download the ISO. Go may need network access to obtain the toolchain or dependencies if they are not cached.

For a small standalone exercise with only Python's standard library, the following scanner locates the raw `info` slice and hashes it. It is intentionally a fixture-learning tool, not a hardened parser for arbitrary input:

```bash
python3 - <<'PY'
from pathlib import Path
import hashlib

path = Path('internal/protocol/metainfo/examples/ubuntu-23.04-desktop-amd64.iso.torrent')
data = path.read_bytes()

def read_string(pos):
    colon = data.index(b':', pos)
    start = colon + 1
    end = start + int(data[pos:colon])
    return data[start:end], end

def skip(pos):
    token = data[pos:pos + 1]
    if token == b'i':
        return data.index(b'e', pos) + 1
    if token in (b'l', b'd'):
        pos += 1
        while data[pos:pos + 1] != b'e':
            pos = skip(pos)
        return pos + 1
    return read_string(pos)[1]

assert data[:1] == b'd'
pos = 1
while data[pos:pos + 1] != b'e':
    key, pos = read_string(pos)
    end = skip(pos)
    if key == b'info':
        raw_info = data[pos:end]
        digest = hashlib.sha1(raw_info).hexdigest()
        print('Torrent bytes:', len(data))
        print('Info bytes:', len(raw_info))
        print('Info hash:', digest)
        print('Magnet:', 'magnet:?xt=urn:btih:' + digest)
        break
    pos = end
else:
    raise ValueError('No info dictionary found')
PY
```

Expected output for the bundled fixture:

```text
Torrent bytes: 376676
Info bytes: 376419
Info hash: 443c7602b4fde83d1154d6d9da48808418b181b6
Magnet: magnet:?xt=urn:btih:443c7602b4fde83d1154d6d9da48808418b181b6
```

Compare the metadata size to the payload size. Then inspect [`ParseMetaInfoBytes`](../internal/protocol/metainfo/parse.go) to see the same raw-byte identity check in the application.

## 2. Trace one hash with source navigation

Use these searches from the repository root:

```bash
rg -n 'func .*runSampleInfoHashes|func .*runInfoHashTriage|func .*requestPeersForHash' internal/dhtcrawler
rg -n 'func .*Request|func btHandshake|func exHandshake|func readAllPieces' internal/protocol/metainfo/metainforequester
rg -n 'func .*runPersistTorrents|func createTorrentModel|func .*Process' internal/dhtcrawler internal/processor
```

At each boundary, write down what data has become available:

1. Sample: only a hash and the sampling node's UDP address.
2. Lookup: potential peer endpoints.
3. Metadata response: name, file structure, size, and payload piece hashes.
4. Commit: durable torrent and queued processing work.
5. Classification: application-derived search attributes and optional content match.

This exercise makes it clear which layer can answer a question and which layer cannot.

## 3. Inspect or start a configured instance

With a built `bitmagnet` binary in the repository root:

```bash
./bitmagnet worker list
./bitmagnet config show
```

`config show` prints resolved configuration, defaults, and resolver sources. Treat its output as local configuration information; it may contain credential values.

With PostgreSQL configured and available, the explicit worker command is:

```bash
./bitmagnet worker run --keys=http_server --keys=queue_server --keys=dht_crawler
```

Or use `worker run --all`. These commands start real network activity. Database settings must match your environment; see the project's [existing installation guide](../bitmagnet.io/setup/installation.md) and the configuration source under [internal/config](../internal/config).

The root [docker-compose.yml](../docker-compose.yml) is a full VPN/observability example with provider placeholders, not a ready-to-run minimal setup. Read and configure it before using it.

## 4. Configuration controls to understand first

| Control | Default in source | Practical effect |
| --- | --- | --- |
| `dht_crawler.scaling_factor` | 10 | Multiplies pipeline capacities and concurrency |
| `dht_crawler.save_files_threshold` | 100 | Limits persisted multi-file entries |
| `dht_crawler.save_pieces` | false | Controls storage of payload piece hashes |
| `dht_crawler.rescrape_threshold` | 720 hours | Age test when a known hash re-enters triage |
| `dht_server.port` | 3334 | Local DHT UDP port |
| `dht_server.query_timeout` | 4 seconds | DHT RPC deadline |
| Metadata requester `RequestTimeout` | 6 seconds | Inner peer request deadline |
| `http_server.local_address` | `:3333` | HTTP listener |

Use `config show` for the effective values in your installation. The bootstrap reseed interval is the exception documented in [Chapter 2](02-dht-crawler.md): the crawler factory hard-codes ten minutes despite the config field.

Do not confuse the HTTP port, local DHT port, and remote peer TCP ports. The metadata requester opens outbound TCP connections to addresses returned by DHT; it does not always connect to remote port 3334. Publishing a TCP port in Compose is not evidence that this metadata requester serves incoming payload connections.

## 5. Observe network traffic

For an already-running local crawler using the default UDP port:

```bash
tcpdump -ni any -c 50 'udp port 3334'
```

Packet capture requires the appropriate local privileges. With container or VPN networking, capture in the relevant network namespace or interface. A host capture may show only the VPN tunnel's encrypted traffic rather than the DHT packets inside it.

Look for bencoded method names such as `find_node`, `sample_infohashes`, and `get_peers`. A packet analyzer may decode these automatically; otherwise inspect the UDP payload.

For metadata traffic, identify a peer endpoint from local debugging and follow that TCP stream. The expected landmarks are the protocol handshake, extension negotiation, and `ut_metadata` requests/responses. TCP segmentation means a packet is not necessarily one application message.

## 6. Database observation

Against your own database, these read-only queries separate ingestion from processing:

```sql
SELECT info_hash, name, size, files_status, files_count
FROM torrents
ORDER BY created_at DESC
LIMIT 10;

SELECT queue, status, count(*)
FROM queue_jobs
GROUP BY queue, status
ORDER BY queue, status;

SELECT source, seeders, leechers, updated_at
FROM torrents_torrent_sources
ORDER BY updated_at DESC
LIMIT 10;
```

A pending job with a future `run_after` is intentionally delayed. A growing eligible backlog suggests missing or slow queue workers. Counts may be null when scraping has not succeeded. A timestamp records a local observation or update; it is not necessarily the original torrent publication date.

## 7. Diagnose the pipeline in order

| Symptom | First things to inspect |
| --- | --- |
| No routing nodes | Worker enabled, bootstrap DNS, UDP socket, outbound/reply reachability |
| Routing nodes but few samples | Sampling RPC results, candidate timing, remote extension support, deduplication |
| Samples but no peers | `get_peers` values; remember there is no recursive lookup for that hash |
| Peers but no metadata | TCP reachability, extension support, request deadline, rejection, hash mismatch |
| Metadata but no persisted torrents | Banning checker, batch timer, PostgreSQL transaction errors |
| Torrent rows but no classified results | Queue worker, job eligibility/status, classifier errors |
| Missing or stale swarm counts | BEP 33 response support, separate scrape writes, rescrape age and rediscovery |
| Truncated file lists | `files_status`, total `files_count`, configured retention threshold |
| Many repeats after restart | In-memory deduplication and routing state were rebuilt |
| Slower after increasing scaling | Peer timeouts, CPU, sockets, database capacity, channel backpressure |

Use the [observability configuration](../observability) and metric collectors next to each subsystem. The crawler exposes `bitmagnet_dht_crawler_persisted_total` with an `entity` label for torrent and source writes. It counts persistence activity and should not be read as the current number of unique searchable torrents.

## 8. Small learning experiments

These are proposed exercises, not changes made to the application:

- Change only a magnet's display-name hint and compare the info hash: it remains identical.
- In a temporary copy of a torrent, change an outer field and recompute the raw info hash: it remains identical if `info` is untouched.
- Change a byte inside the info dictionary and recompute the hash: identity changes.
- Follow a `get_peers` response containing nodes but no peer values through `requestPeersForHash`: this attempt ends rather than recursively querying each returned node.
- Read the metadata assembly code and design cases for duplicate blocks, invalid indices, and an early short final block.
- Compare a stored torrent with its derived classification to separate network metadata from application inference.

Return to the [end-to-end map](README.md) after each exercise and identify which edge you just studied.
