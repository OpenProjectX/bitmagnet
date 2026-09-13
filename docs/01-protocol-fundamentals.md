# Protocol fundamentals

[Documentation index](README.md) · Next: [DHT crawler](02-dht-crawler.md)

## 1. Separate the layers

| Concept | What it provides | Role in this project |
| --- | --- | --- |
| BitTorrent peer protocol | Communication between participants in a torrent swarm | Fetches metadata from a peer over TCP |
| DHT | Distributed lookup of peer addresses by info hash | Discovers hashes and locates metadata-serving peers |
| `.torrent` file | Bencoded metainfo with an `info` dictionary and optional outer fields | Offline parsing example and a way to understand metadata |
| Magnet URI | A textual reference to content, optionally with hints | Generated from indexed torrent records |
| Tracker | A service that returns peers for a hash | Useful protocol background; not used by the crawler path traced here |
| Bitmagnet database | Names, file lists, classifications, and observations | Supports keyword search and filtering |

A normal download may start from a magnet, find peers, retrieve metadata, and request payload blocks. Bitmagnet's crawler starts from sampled hashes and stops after acquiring metadata and observations.

## 2. Three different identifiers

The repository uses [`protocol.ID`](../internal/protocol/id.go), a `[20]byte`, for several concepts. Identical representation does not mean identical purpose.

- **Info hash:** identifies a torrent's info dictionary.
- **DHT node ID:** identifies a routing participant in the 160-bit DHT keyspace.
- **Peer ID:** identifies a BitTorrent client in the peer handshake.

The info hash is passed through `nodeHasPeersForHash`, `infoHashWithPeers`, and `infoHashWithMetaInfo` in [`crawler.go`](../internal/dhtcrawler/crawler.go). The accompanying `node` is a DHT UDP endpoint; the `peers` are endpoints for subsequent BitTorrent connections. Do not assume their ports are interchangeable.

A 20-byte value appears as 40 hexadecimal characters in logs and magnet links. `ParseID` accepts hex, optionally prefixed with `0x`; it is not a general magnet parser.

## 3. Bencoding and torrent identity

Bencoding represents integers, byte strings, lists, and dictionaries. A v1 torrent identifies its **exact encoded `info` bytes** with SHA-1:

```text
info_hash = SHA1(raw_bencoded_info_dictionary)
```

It does not hash the entire `.torrent` file or directly hash the payload file. A single-file info dictionary describes a name, byte length, piece length, and concatenated payload piece hashes; a multi-file dictionary has a file list instead of the single length. Outer fields can include tracker URLs. See [BEP 3](https://www.bittorrent.org/beps/bep_0003.html).

Small encoding examples:

```text
4:spam          byte string: spam
i42e            integer 42
l1:a1:be        list: [a, b]
d1:ai1ee        dictionary: {a: 1}
```

Byte strings may hold binary hashes, not human-readable text. Their prefix counts bytes. Dictionary keys are ordered by raw bytes.

The implementation in [`ParseMetaInfoBytes`](../internal/protocol/metainfo/parse.go) hashes received bytes before unmarshalling. Re-encoding a parsed dictionary is unnecessary for this check and could change the bytes being checked.

Changing a title inside `info` changes the hash. Changing only an outer tracker URL leaves it unchanged. Two torrents containing the same payload can have different hashes if their metadata or piece layout differs.

## 4. DHT lookup and XOR distance

Conceptually, a DHT node provides a mapping:

```text
info hash -> peer contact records known to that node
```

Routing compares node IDs and target keys with XOR distance. In a shortened eight-bit example, the distance from `10110000` to `10110011` is `00000011` (3), while its distance to `00110000` is `10000000` (128). “Close” refers to this numeric space, not geography or network latency.

A normal iterative lookup asks known nodes and follows contacts closer to a target. `find_node` returns DHT nodes; `get_peers` can return peers or closer nodes. `announce_peer` registers contact information using a token acquired from a preceding lookup. These are bencoded KRPC messages over UDP. See [BEP 5](https://www.bittorrent.org/beps/bep_0005.html).

The wire fields include `t` (transaction ID), `y` (query/response/error), `q` (method), and `a` (arguments). For example, this is a **decoded schematic**, not literal JSON sent over the network:

```json
{
  "t": "aa",
  "y": "q",
  "q": "get_peers",
  "a": {"id": "<20 raw bytes>", "info_hash": "<20 raw bytes>"}
}
```

Bitmagnet's [`msg.go`](../internal/protocol/dht/msg.go) defines these structures. Its crawler does not implement a complete iterative peer lookup for each sampled hash: it asks the sampling node once and feeds returned nodes into general discovery. [Chapter 2](02-dht-crawler.md) explains the consequence.

## 5. Discovery without already knowing a hash

Ordinary peer lookup requires a known hash. Bitmagnet instead uses `sample_infohashes`, an indexing extension that returns samples from a node's stored hash keys, alongside node contacts and scheduling information. Its `target` guides the returned node contacts; it does not perform a keyword search or select hashes by title. See [BEP 51](https://www.bittorrent.org/beps/bep_0051.html).

A sample is only a lead. The corresponding peers may be offline, unreachable, or unable to provide metadata. This is why “hashes discovered” and “torrents indexed” represent different stages.

There is also a tracker-specific discovery exception: some HTTP trackers expose
**full scrape**, returning tracked hashes and swarm counters without requiring
a hash in the request. This does not return the torrent info dictionary, and
Bitmagnet does not currently use it. See the
[full-scrape guide and survey](06-tracker-lists.md).

## 6. What a magnet link contains

A typical shape is:

```text
magnet:?xt=urn:btih:<40-hex-info-hash>&dn=Example.iso&xl=123456
```

`xt` gives the exact topic, `btih` denotes a BitTorrent info hash, `dn` supplies a display name, and `xl` supplies a size hint. Magnets may also include tracker hints using `tr`; Bitmagnet's [`Torrent.MagnetURI`](../internal/model/torrents.go) emits `xt`, a URL-escaped `dn`, and `xl`, without a tracker list. The v1 magnet convention and metadata bootstrap are described in [BEP 9](https://www.bittorrent.org/beps/bep_0009.html).

A magnet is not a server address, an embedded file list, or proof that a file is available. The client still needs peers and verified metadata. Its display name is a hint; changing `dn` does not change the referenced hash.

Clicking a magnet from Bitmagnet hands the reference to the user's registered torrent client. That client's discovery and download activity is separate from Bitmagnet's indexing pipeline.

## 7. Metadata pieces versus payload pieces

These are different units with different purposes:

| Unit | Contents | Used here? |
| --- | --- | --- |
| Metadata transfer piece | A block of encoded info dictionary bytes | Yes, retrieved through `ut_metadata` |
| Payload piece | A range of the actual shared file data | No payload retrieval in this requester |
| Entry in the metadata `pieces` field | A hash for a payload piece | Received within metadata; optionally persisted |

The metadata transfer block size is 16 KiB, with a potentially shorter final block. Extension negotiation happens before those requests; see [BEP 9](https://www.bittorrent.org/beps/bep_0009.html) and [BEP 10](https://www.bittorrent.org/beps/bep_0010.html). The torrent's own payload piece length can be entirely different.

## 8. Scope: v1, v2, and private torrents

This crawler is centered on 20-byte SHA-1 identities and generates `urn:btih` magnets. Do not infer full v2 support from the presence of an extension-bit constant or a dependency that supports v2. BitTorrent v2 uses SHA-256-based metadata identity and a different file/piece structure; see [BEP 52](https://www.bittorrent.org/beps/bep_0052.html). A hybrid torrent's v1 side is distinct from implementing pure-v2 discovery.

The crawler copies the metadata's `private` flag into its database model. That is not a mechanism for discovering private tracker catalogs. Public DHT observations determine what this pipeline can see.

## Check your understanding

1. Can a DHT node return a title for an arbitrary hash? The DHT lookup supplies contacts; titles come from metadata.
2. Does an indexed name prove payload integrity? No. Hash checking authenticates the retrieved metadata against the requested hash, not a downloaded payload.
3. Why can a magnet work without a tracker URL? A downloader can use DHT discovery.
4. Does `save_pieces` mean Bitmagnet stores movie data? No. It stores the metadata's payload piece hashes and piece length.
