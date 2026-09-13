#!/usr/bin/env python3
"""Fetch tracker URLs for torrent clients; Bitmagnet cannot import tracker lists."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sys
import tempfile
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


SOURCES = (
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all.txt",
    "https://raw.githubusercontent.com/XIU2/TrackersListCollection/master/all.txt",
)
MAX_BYTES = 2 * 1024 * 1024
SCHEMES = {"udp", "http", "https", "ws", "wss"}


def parse_trackers(text):
    """Validate every entry, preserving URL paths and queries exactly."""
    trackers = []
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            url = urlsplit(line)
            valid = (
                url.scheme in SCHEMES
                and bool(url.hostname)
                and url.username is None
                and url.password is None
                and not url.fragment
                and not any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in line)
                and "\\" not in line
                and (url.port is None or 1 <= url.port <= 65535)
                and (url.scheme != "udp" or url.port is not None)
            )
        except ValueError:
            valid = False
        if not valid:
            raise ValueError(f"invalid tracker URL on line {number}")
        trackers.append(line)
    if not trackers:
        raise ValueError("source contains no tracker URLs")
    return trackers


def fetch(source, timeout):
    request = Request(source, headers={"User-Agent": "bitmagnet-tracker-list-fetcher/1.0"})
    with urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise ValueError(f"unexpected HTTP status {response.status}")
        data = response.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("source exceeds 2 MiB limit")
    return parse_trackers(data.decode("utf-8-sig"))


def write_atomic(destination, trackers):
    """Replace the previous list only after all downloads and parsing succeed."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="\n",
            dir=destination.parent, prefix=f".{destination.name}.", delete=False,
        ) as output:
            temp_path = Path(output.name)
            output.write("\n\n".join(trackers) + "\n")
        os.replace(temp_path, destination)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Fetch and merge ngosang and XIU2 tracker lists for torrent clients.",
        epilog=("Bitmagnet has no tracker-list import API. This script writes a local "
                "file; it does not submit tracker URLs to /import or configure DHT nodes."),
    )
    parser.add_argument("--output", type=Path, default=Path("/tmp/bitmagnet-trackers.txt"))
    parser.add_argument("--timeout", type=float, default=20, help="HTTP timeout in seconds")
    parser.add_argument(
        "--schemes", nargs="+", choices=sorted(SCHEMES), default=sorted(SCHEMES),
        help="Keep only these tracker protocols (default: all)",
    )
    args = parser.parse_args(argv)
    if not 0 < args.timeout < float("inf"):
        parser.error("--timeout must be a finite positive number")

    try:
        with ThreadPoolExecutor(max_workers=len(SOURCES)) as pool:
            # map preserves source order, including the first list's ranking.
            lists = list(pool.map(lambda source: fetch(source, args.timeout), SOURCES))
        for source, entries in zip(SOURCES, lists):
            print(f"Fetched {len(entries)} entries: {source}", file=sys.stderr)
        merged = list(dict.fromkeys(
            entry for entries in lists for entry in entries
            if urlsplit(entry).scheme in args.schemes
        ))
        if not merged:
            raise ValueError("no tracker URLs match the selected protocols")
        write_atomic(args.output, merged)
    except (OSError, ValueError) as error:
        print(f"Failed; output was not replaced: {error}", file=sys.stderr)
        return 1

    print(f"Saved {len(merged)} unique tracker URLs to {args.output}")
    print("Bitmagnet import: unsupported. These are tracker URLs, not torrent records.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
