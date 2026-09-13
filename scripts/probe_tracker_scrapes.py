#!/usr/bin/env python3
"""Bounded HTTP full-scrape survey. No announces, peer downloads, or imports."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen
import zlib

from fetch_trackers import parse_trackers

LIMIT = 256 * 1024
DECODE_LIMIT = 1024 * 1024


class Incomplete(ValueError):
    pass


def decode(data, pos=0, depth=0):
    if depth > 20:
        raise ValueError('bencode nesting limit')
    if pos >= len(data):
        raise Incomplete()
    token = data[pos:pos+1]
    if token == b'i':
        end = data.find(b'e', pos + 1)
        if end < 0:
            raise Incomplete()
        return int(data[pos+1:end]), end + 1
    if token in (b'd', b'l'):
        result = {} if token == b'd' else []
        pos += 1
        while True:
            if pos >= len(data):
                raise Incomplete()
            if data[pos:pos+1] == b'e':
                return result, pos + 1
            value, pos = decode(data, pos, depth+1)
            if token == b'd':
                if not isinstance(value, bytes):
                    raise ValueError('non-string dictionary key')
                child, pos = decode(data, pos, depth+1)
                result[value] = child
            else:
                result.append(value)
    if not token.isdigit():
        raise ValueError('not bencode')
    colon = data.find(b':', pos)
    if colon < 0:
        raise Incomplete()
    length = int(data[pos:colon])
    end = colon + 1 + length
    if length < 0:
        raise ValueError('negative string length')
    if end > len(data):
        raise Incomplete()
    return bytes(data[colon+1:end]), end


def inspect_prefix(data):
    """Confirm a real hash/statistics entry without downloading a whole catalog."""
    if not data:
        raise Incomplete()
    if data[:1] != b'd':
        raise ValueError('response is not a bencoded dictionary')
    pos = 1
    while True:
        if pos >= len(data):
            raise Incomplete()
        if data[pos:pos+1] == b'e':
            return 'no_files_dictionary', 'No files dictionary in response'
        key, pos = decode(data, pos)
        if key == b'files':
            if pos >= len(data):
                raise Incomplete()
            if data[pos:pos+1] != b'd':
                raise ValueError('files is not a dictionary')
            pos += 1
            if pos >= len(data):
                raise Incomplete()
            if data[pos:pos+1] == b'e':
                return 'empty_files', 'Empty files dictionary; enumeration not demonstrated'
            info_hash, pos = decode(data, pos)
            stats, pos = decode(data, pos)
            if not isinstance(info_hash, bytes) or len(info_hash) != 20:
                raise ValueError('scrape key is not a 20-byte info hash')
            if not isinstance(stats, dict) or not all(
                type(stats.get(k)) is int and stats[k] >= 0
                for k in (b'complete', b'incomplete', b'downloaded')
            ):
                raise ValueError('hash entry lacks valid scrape counters')
            return 'hashes_observed', 'Validated one hash with scrape counters; stopped reading early'
        value, pos = decode(data, pos)
        if key in (b'failure reason', b'failure_reason'):
            reason = value.decode('utf-8', errors='replace') if isinstance(value, bytes) else str(value)
            return 'tracker_failure', reason[:200]


def scrape_url(announce):
    parts = urlsplit(announce)
    if parts.scheme not in ('http', 'https'):
        return None
    # BEP 48 mapping. Do not invent HTTP endpoints for UDP-only URLs.
    if 'announce' not in parts.path:
        return None
    return urlunsplit(parts._replace(path=parts.path.replace('announce', 'scrape', 1)))


def probe(url):
    result = {'scrape_url': url}
    start = time.monotonic()
    wire = bytearray()
    decoded = bytearray()
    try:
        request = Request(url, headers={
            'User-Agent': 'bitmagnet-full-scrape-survey/1.0',
            'Accept-Encoding': 'gzip',
        })
        with urlopen(request, timeout=6) as response:
            result.update(http_status=response.status, final_url=response.url)
            encoding = response.headers.get('Content-Encoding', '').lower()
            if encoding not in ('', 'identity', 'gzip'):
                raise ValueError(f'unsupported content encoding: {encoding}')
            unzip = zlib.decompressobj(16 + zlib.MAX_WBITS) if encoding == 'gzip' else None
            while len(wire) < LIMIT and len(decoded) < DECODE_LIMIT:
                if time.monotonic() - start > 12:
                    result.update(status='budget_exceeded', detail='Read time budget exceeded')
                    break
                block = response.read1(min(8192, LIMIT-len(wire)))
                if not block:
                    result.update(status='incomplete_response', detail='No conclusive scrape entry before EOF')
                    break
                wire.extend(block)
                decoded.extend(unzip.decompress(block, DECODE_LIMIT-len(decoded)) if unzip else block)
                try:
                    status, detail = inspect_prefix(decoded)
                except Incomplete:
                    continue
                result.update(status=status, detail=detail)
                break
            else:
                result.update(status='budget_exceeded', detail='Response byte limit reached')
    except HTTPError as error:
        result.update(status='http_error', http_status=error.code, detail=str(error))
    except (OSError, ValueError, zlib.error) as error:
        result.update(status='unconfirmed', detail=str(error)[:300])
    result.update(bytes_read=len(wire), decoded_bytes=len(decoded), elapsed_seconds=round(time.monotonic()-start, 2))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    entries = list(dict.fromkeys(parse_trackers(args.input.read_text())))
    candidates = {}
    skipped = []
    for entry in entries:
        url = scrape_url(entry)
        if url:
            candidates.setdefault(url, []).append(entry)
        else:
            skipped.append({'announce_url': entry, 'reason': 'No standard HTTP full-scrape mapping'})
    results = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        for result in pool.map(probe, candidates):
            result['announce_urls'] = candidates[result['scrape_url']]
            results.append(result)
            print(result['status'], result['scrape_url'], flush=True)
    report = {
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'input_urls': len(entries),
        'protocol_counts': dict(Counter(urlsplit(e).scheme for e in entries)),
        'http_candidates': len(candidates),
        'status_counts': dict(Counter(r['status'] for r in results)),
        'method': 'One GET without info_hash per mapped HTTP(S) endpoint; stop at first validated hash/statistics entry. No retries. Verified TLS; environment proxy settings apply.',
        'limits': {'workers': 6, 'socket_timeout_seconds': 6, 'read_budget_seconds': 12, 'wire_bytes_per_endpoint': LIMIT, 'decoded_bytes_per_endpoint': DECODE_LIMIT},
        'results': results, 'skipped': skipped,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report['status_counts'], sort_keys=True))


if __name__ == '__main__':
    main()
