# Tracker full-scrape survey — 2026-09-13

[Tracker guide](../06-tracker-lists.md) · [Raw JSON evidence](tracker-scrape-survey-2026-09-13.json)

Observed at `2026-09-13T02:28:53.086448+00:00` from the development workspace network, not from a Kubernetes Bitmagnet pod.

## Results

**11 of 74 tested HTTP(S) endpoints returned at least one valid info-hash/statistics entry without a supplied hash.** This demonstrates hash enumeration behavior. It does not demonstrate retrieval of torrent names, file lists, or the complete info dictionary.

The fresh merged input contained 148 exact-unique URLs: 52 HTTP, 22 HTTPS, 73 UDP, and 1 WSS. Both GitHub lists were fetched successfully. The 74 UDP/WSS URLs were not tested for HTTP full scrape, and are not counted as failures.

| Observed result | Endpoints |
| --- | ---: |
| HTTP error | 17 |
| Timeout, connection/TLS failure, or non-bencoded response | 32 |
| Hash/statistics entry observed | 11 |
| Explicit tracker failure response | 9 |
| Empty files dictionary; inconclusive enumeration | 2 |
| Bencoded response without files dictionary | 3 |

## Endpoints with positive evidence

| Scrape endpoint | HTTP status | Bytes read |
| --- | ---: | ---: |
| `https://tracker.nekomi.cn:443/scrape` | 200 | 1022 |
| `https://004430.xyz:443/scrape` | 200 | 1022 |
| `http://bittorrent-tracker.e-n-c-r-y-p-t.net:1337/scrape` | 200 | 8192 |
| `http://004430.xyz:80/scrape` | 200 | 8186 |
| `https://t.213891.xyz:443/scrape` | 200 | 102 |
| `http://107.189.2.131:1337/scrape` | 200 | 8192 |
| `http://207.241.226.111:6969/scrape` | 200 | 8096 |
| `http://207.241.231.226:6969/scrape` | 200 | 2802 |
| `http://bt1.archive.org:6969/scrape` | 200 | 8192 |
| `http://bt2.archive.org:6969/scrape` | 200 | 4000 |
| `http://retracker01-msk-virt.corbina.net:80/scrape` | 200 | 8096 |

These are 11 URLs across 10 distinct hostname/IP strings. HTTP/HTTPS variants and hostname/IP aliases can refer to the same backend, so this is not a count of independent tracker operators.

## Explicit tracker responses

| Endpoint | Reported reason |
| --- | --- |
| `https://tracker.bt4g.com:443/scrape` | missing info_hash |
| `http://tracker.bt4g.com:2095/scrape` | missing info_hash |
| `https://tr.nyacat.pw:443/scrape` | Invalid request: no query string |
| `http://tracker.waaa.moe:6969/scrape` | no info_hash parameter supplied |
| `http://tracker.privateseedbox.xyz:2710/scrape` | scrape requires query string |
| `http://tracker.nexusstream.eu:6969/scrape` | no info_hash parameter supplied |
| `http://tr.nyacat.pw:80/scrape` | Invalid request: no query string |
| `http://tracker.xn--djrq4gl4hvoi.top:80/scrape` | no info_hash parameter supplied |
| `https://tracker.onetracker.net:443/scrape` | Scrape not supported |

## Method and limits

- Map `announce` to `scrape` in listed HTTP(S) URL paths; issue a GET without `info_hash`.
- Require a bencoded files dictionary, a 20-byte hash key, and nonnegative integer complete/incomplete/downloaded counters.
- Stop reading on the first validated entry. Large complete catalogs were not fetched, counted, or imported.
- Use six concurrent workers, six-second socket timeouts, a read budget checked after twelve seconds, and 256 KiB received/1 MiB decoded byte limits per endpoint. Slow individual reads or DNS can exceed the checked time budget.
- Keep TLS verification enabled. Proxy environment settings are honored. Network failures describe this vantage point, not universal tracker availability.
- The retained JSON records the final survey. A preliminary development pass was discarded after fixing bytearray handling in the parser; endpoints may have received two requests across development and the final pass. The probe has no automatic retries.
- A positive first entry proves an endpoint returned an unsolicited hash with statistics; it does not prove the response would enumerate every tracked hash or that the advertised peers are reachable.
- Empty responses and network/HTTP failures remain inconclusive for capability. Some failures explicitly state that a hash/query is required.
- Torrent metadata exchange was not tested: ordinary tracker scrape carries swarm statistics, while verified torrent metadata must be acquired separately from peers.
- No tracker announce, peer metadata/payload transfer, or Bitmagnet database import was performed.

## Reproduction

```bash
python3 scripts/fetch_trackers.py --output /tmp/trackers-survey.txt
python3 scripts/probe_tracker_scrapes.py /tmp/trackers-survey.txt --output /tmp/survey.json
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p "test_probe_tracker_scrapes.py" -v
```

Sources: [ngosang trackerslist](https://github.com/ngosang/trackerslist), [XIU2 collection](https://github.com/XIU2/TrackersListCollection). Protocol background: [HTTP scrape fields](https://www.bittorrent.org/beps/bep_0048.html), [opentracker full-scrape support](https://erdgeist.org/arts/software/opentracker/).
