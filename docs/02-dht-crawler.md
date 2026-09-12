# DHT crawler internals

[Documentation index](README.md) · Previous: [Fundamentals](01-protocol-fundamentals.md) · Next: [Metadata exchange](03-metadata-exchange.md)

## 1. Components and state

[`crawler.go`](../internal/dhtcrawler/crawler.go) starts the pipeline goroutines; [`factory.go`](../internal/dhtcrawler/factory.go) constructs their channels and registers the `dht_crawler` worker.

Three kinds of state serve different purposes:

| State | Location | Purpose |
| --- | --- | --- |
| Routing nodes and hash-to-peer contacts | In-memory `ktable` | Network discovery and DHT responses |
| Recently encountered hashes | In-memory stable Bloom filter | Reduce repeated work and database lookups |
| Torrent catalog and processing jobs | PostgreSQL | Durable search data and downstream processing |

Restarting rebuilds discovery state. It does not erase the durable catalog.

[`ktable`](../internal/protocol/dht/ktable/doc.go) maintains separate node and hash keyspaces using a binary-tree implementation. [`factory.go`](../internal/protocol/dht/ktable/factory.go) sets both K parameters to 80. These are routing data-structure parameters, not a limit of 80 indexed torrents. A reverse address map supports filtering and removal by IP.

## 2. Bootstrap and node maintenance

[`bootstrap.go`](../internal/dhtcrawler/bootstrap.go) resolves configured bootstrap hostnames to UDP addresses and queues pings immediately at startup. Bootstrap contacts introduce the crawler to other nodes; they do not contain a centralized torrent catalog.

[`ping.go`](../internal/dhtcrawler/ping.go) handles liveness, while [`find_node.go`](../internal/dhtcrawler/find_node.go) queries nodes and feeds returned contacts into discovery. Successful responses refresh routing entries; failures can drop entries.

[`discovered_nodes.go`](../internal/dhtcrawler/discovered_nodes.go) batches contacts, deduplicates them by IP, filters known addresses, and sends each unknown contact to one available ping, find-node, or sampling channel. The `select` chooses one send; it does not send the contact to all three.

Successful inbound DHT requests also introduce their senders through the [`responderNodeDiscovery`](../internal/protocol/dht/responder/node_discovery.go) wrapper. That path discovers **nodes**; it does not directly enqueue every inbound `get_peers` info hash for metadata retrieval.

The crawler rotates `soughtNodeID` every ten seconds. This changes the target passed to discovery queries. The actual local DHT identity is separately created by [`dhtfx`](../internal/protocol/dht/dhtfx/module.go), using `RandomNodeIDWithClientSuffix`.

### Configuration discrepancy

The config declares `ReseedBootstrapNodesInterval` with a one-minute default. The crawler factory actually assigns a hard-coded ten-minute interval. Changing that config field alone therefore does not change the interval in this checkout. This is an implementation observation, not a protocol requirement.

## 3. Hash sampling

[`sample_infohashes.go`](../internal/dhtcrawler/sample_infohashes.go) periodically selects sampling candidates, sends the RPC, and processes `samples`, `num`, `interval`, and returned nodes. [BEP 51](https://www.bittorrent.org/beps/bep_0051.html) describes the sample and interval semantics.

For each newly encountered sample, the crawler records:

```text
nodeHasPeersForHash {
    infoHash: sampled torrent identifier
    node: UDP address of the node that supplied the sample
}
```

It passes this record to database triage. Returned nodes separately enrich general discovery, with a bounded attempt to enqueue them.

Two scheduling choices are specific to this implementation:

- When new hashes were discovered and the response interval exceeds 300 seconds, the crawler substitutes 60 seconds. This departs from BEP 51's instruction to wait out the supplied interval.
- [`NodeSampleInfoHashesRes`](../internal/protocol/dht/ktable/node.go) adds five minutes of delay when no new hashes were discovered.

An RPC failure drops the node. The adapter also tolerates absent sampling fields, returning zero values; the crawler marks successful calls as supporting BEP 51. Consequently, that flag is not strict proof that a response included the standard sampling fields.

## 4. Deduplication and database triage

The stable Bloom filter is probabilistic. Its `testAndAdd` operation runs under a mutex. A positive result suppresses a sample before database access, but false positives are possible and stable filters age out older observations. The comment saying it contains every encountered hash should not be interpreted as an exact permanent set.

[`infohash_triage.go`](../internal/dhtcrawler/infohash_triage.go) deduplicates each batch, applies the blocking manager, and queries torrents with their `dht` source observations.

| Condition | Next action |
| --- | --- |
| Hash absent from the database | Request peers and metadata |
| File status is `no_info` | Request peers and metadata |
| Non-single torrent lacks a known file count | Request peers and metadata |
| Previously over threshold, but now fits the configured threshold | Request peers and metadata |
| Metadata is sufficient but counts are missing or old | Scrape |
| Metadata and source counts are sufficiently current | Discard this discovery |

A block-filter or database-query failure logs an error and abandons that batch. The channel pipeline is not a durable retry queue. A hash can be encountered again later, subject to deduplication and network discovery.

## 5. Peer lookup: the important shortcut

[`requestPeersForHash`](../internal/dhtcrawler/get_peers.go) sends `get_peers` to the node that supplied the sample. Its response contains `values` for peers and/or `nodes` for DHT contacts.

This function feeds returned node contacts into general discovery, but **does not recursively query those contacts for the same hash**. If `values` is empty, it returns `no peers found` and that attempt ends. This is a deliberate description of the code path; it is less exhaustive than the general iterative lookup model.

When peers exist, the crawler stores them in the in-memory hash table and sends the hash plus peer list to the metadata stage. Peer records in the routing table are distinct from permanent torrent records in PostgreSQL.

## 6. UDP transport and incoming service

[`server.go`](../internal/protocol/dht/server/server.go) owns the shared UDP query/response machinery:

1. Allocate a transaction ID and a buffered response channel.
2. Register the channel in a mutex-protected map.
3. Bencode and send the query.
4. Wait for a matching response or the query deadline.
5. Remove the pending transaction entry.

The receive loop decodes datagrams and dispatches incoming queries to the responder or replies to the transaction map. The default query timeout is four seconds, and the configured DHT port is 3334. These deadlines apply to DHT RPCs, not peer metadata retrieval.

The [`responder`](../internal/protocol/dht/responder/responder.go) supports:

| RPC | Local behavior |
| --- | --- |
| `ping` | Return local node ID |
| `find_node` | Return closest known routing nodes |
| `get_peers` | Return cached peers if present, routing contacts, and an announce token |
| `announce_peer` | Validate the token and cache the announcing peer |
| `sample_infohashes` | Return samples and nodes from the local in-memory table |

Announce tokens bind the hash, local and requester IDs, requester IP, and a process secret through a digest. `AnnouncePort` handles the announced versus implied port. These tokens are for DHT announce validation; they are unrelated to application API credentials.

The incoming responder does not generate the BEP 33 Bloom-filter scrape fields, even though the outbound client can request them. Protocol support can be asymmetric between requesting and serving.

## 7. Scraping swarm estimates

[`scrape.go`](../internal/dhtcrawler/scrape.go) sends `get_peers` with `scrape: 1`. The adapter requires both `BFsd` and `BFpe`; a legacy response with only ordinary peers is treated as a failure. The Bloom filters encode IP populations, so their sizes are estimates. See [BEP 33](https://www.bittorrent.org/beps/bep_0033.html).

[`createTorrentSourceModel`](../internal/dhtcrawler/persist.go) converts those estimates to integer `Seeders` and `Leechers` fields for source `dht`. It maps `BFpe` directly to the application's leecher field. The quality of that label depends on remote announcement accounting; this is not direct verification that every counted peer lacks the complete payload.

Only the responding node's filters are used here. The crawler does not union observations from a full nearest-node lookup. A zero estimate, an unknown count, and a failed scrape are different situations. Counts can become stale, and the default rescrape threshold is 30 days, evaluated when a hash reaches triage again rather than through a guaranteed full-catalog timer.

## 8. Concurrency, buffering, and backpressure

At the default scaling factor of 10, the factory constructs:

| Stage | Input capacity | Concurrent handlers |
| --- | ---: | ---: |
| Ping | 10 | 10 |
| Find node | 100 | 100 |
| Sample hashes | 100 | 100 |
| Get peers | 100 | 200 |
| Scrape | 100 | 200 |
| Retrieve metadata | 100 | 400 |

The constructor order is **capacity, concurrency**, as defined in [`buffered_concurrent_channel.go`](../internal/concurrency/buffered_concurrent_channel.go). Do not read the first argument as worker count.

Triage batches up to 1,000 records with a 20-second timer; torrent and source persistence batch up to 1,000 with a one-minute timer. [`batching_channel.go`](../internal/concurrency/batching_channel.go) implements size/timer flushing. These timers introduce visible latency; backpressure can add more.

Slow peers occupy metadata handler slots. Slow database writes fill downstream channels and eventually delay upstream discovery. Increasing the scaling factor increases outstanding work and memory pressure, so it does not guarantee proportional throughput.

The metadata stage tries peers sequentially for each hash, while many different hashes can progress concurrently. Shutdown cancels the crawler context, but the code does not establish a durable drain of every in-memory batch. PostgreSQL queue jobs provide durability only after persistence.
