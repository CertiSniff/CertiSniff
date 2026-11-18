"""
ct_log.py - Shared helpers for reading a tile-based Certificate Transparency log.

CertiSniff reads Let's Encrypt's Sycamore log, which uses the "static-ct-api"
format: the log is a set of plain files on a web server, and certificates are
stored in "tiles" of 256 entries each. This module holds the logic that both
backfill.py and stream.py need, so it is written once.

It is not meant to be run directly. Import it:

    import ct_log
    size = ct_log.get_log_size(ct_log.DEFAULT_LOG_URL)

Contents:
    tile_path()          tile number -> path on the server, e.g. 1234567 -> "x001/x234/567"
    get_log_size()       how many entries the log has right now (from its checkpoint)
    last_full_tile()     number of the newest tile that has all 256 entries
    download_tile()      download one tile, with retries
    find_tile_at_time()  binary search for the tile that was logged at a given time
    save_tile()          write a tile (gzip-compressed) into the local output folder
    fetch_tile_to_disk() download + save in one step, skipping tiles already saved
    load_progress() / save_progress()   small JSON file that lets scripts resume
"""

import gzip
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests

# The log we read. Certificates expiring in the first half of 2027 land here.
DEFAULT_LOG_URL = "https://mon.sycamore.ct.letsencrypt.org/2027h1/"

# Identify ourselves to the log operator, as they ask.
USER_AGENT = "CertiSniff/1.0 (NYU CS6513 Big Data student project)"

ENTRIES_PER_TILE = 256

log = logging.getLogger("certisniff")

# One shared HTTP session reuses the same connection, which is faster than
# opening a new connection for every tile.
_session = requests.Session()
_session.headers["User-Agent"] = USER_AGENT


class NotFoundError(Exception):
    """The server says the file doesn't exist (yet).

    The log is hosted on Amazon S3, which answers 403 "Access Denied" (not 404)
    for missing files. For stream.py this usually just means the tile isn't
    full yet.
    """


# ---------------------------------------------------------------------------
# Paths and sizes
# ---------------------------------------------------------------------------

def tile_path(tile_number):
    """Turn a tile number into its path on the server.

    Pad the number with zeros to a multiple of 3 digits, split it into groups
    of 3, and put "x" in front of every group except the last.
        5        -> "005"
        1234     -> "x001/234"
        1234567  -> "x001/x234/567"
    """
    digits = str(tile_number)
    padded_length = ((len(digits) + 2) // 3) * 3
    digits = digits.zfill(padded_length)
    groups = [digits[i:i + 3] for i in range(0, len(digits), 3)]
    return "/".join(["x" + group for group in groups[:-1]] + [groups[-1]])


def last_full_tile(log_size):
    """Newest tile that has all 256 entries. The tile after it is still filling up."""
    return log_size // ENTRIES_PER_TILE - 1


def ms_to_text(milliseconds):
    """Format a timestamp in milliseconds (UTC) as readable text."""
    moment = datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
    return moment.strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------------
# Downloading
# ---------------------------------------------------------------------------

def _get(url, max_attempts=5, timeout=30):
    """Download a URL and return its bytes.

    Retries on network errors, rate limiting (429), and server errors (5xx),
    waiting longer after each failure (2s, 4s, 8s, ...). Raises NotFoundError
    straight away on 403/404, because retrying won't make a missing file appear.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            response = _session.get(url, timeout=timeout)
        except requests.RequestException as error:
            problem = type(error).__name__
        else:
            if response.status_code == 200:
                return response.content
            if response.status_code in (403, 404):
                raise NotFoundError(url)
            problem = "HTTP " + str(response.status_code)

        if attempt < max_attempts:
            wait_seconds = min(2 ** attempt, 60)
            log.warning("%s failed (%s), attempt %d of %d, retrying in %ds",
                        url, problem, attempt, max_attempts, wait_seconds)
            time.sleep(wait_seconds)

    raise RuntimeError("Giving up on " + url + " after " + str(max_attempts) + " attempts")


def get_log_size(log_url):
    """Return the number of entries in the log.

    The checkpoint is a small text file. Line 1 names the log, line 2 is the
    number of entries, line 3 is a hash, then signatures.
    """
    text = _get(log_url + "checkpoint").decode("utf-8")
    return int(text.split("\n")[1])


def download_tile(log_url, tile_number):
    """Download one full data tile and return its bytes (uncompressed)."""
    data = _get(log_url + "tile/data/" + tile_path(tile_number))
    # Some servers send the tile gzip-compressed. Gzip data starts with 1f 8b.
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data


def first_timestamp(tile_data):
    """Timestamp (ms since 1970, UTC) of the first entry in a tile.

    Every entry starts with an 8-byte timestamp, so the first 8 bytes of the
    tile are the first entry's timestamp.
    """
    return int.from_bytes(tile_data[:8], "big")


# ---------------------------------------------------------------------------
# Finding a tile by time
# ---------------------------------------------------------------------------

def find_tile_at_time(log_url, target_ms, newest_tile, sleep_seconds=0.2):
    """Return the last tile whose first entry was logged at or before target_ms.

    Tile names have no dates, but timestamps grow with the tile number, so the
    tiles are sorted by time. Binary search: look at the middle tile, keep the
    half that must contain the answer, repeat. About 19 downloads for 400,000
    tiles. Only the first 8 bytes of each tile are used.
    """
    oldest = first_timestamp(download_tile(log_url, 0))
    if oldest >= target_ms:
        log.info("The whole log is newer than the target time; starting at tile 0")
        return 0

    low, high = 0, newest_tile  # the answer is always somewhere in [low, high]
    step = 0
    while low < high:
        step += 1
        middle = (low + high + 1) // 2  # round up so the loop always shrinks
        middle_time = first_timestamp(download_tile(log_url, middle))
        if middle_time <= target_ms:
            low = middle        # middle is early enough; answer is middle or later
            verdict = "before target -> search later tiles"
        else:
            high = middle - 1   # middle is too new; answer is earlier
            verdict = "after target  -> search earlier tiles"
        log.info("Search step %2d: tile %9d starts %s (%s)",
                 step, middle, ms_to_text(middle_time), verdict)
        time.sleep(sleep_seconds)
    return low


# ---------------------------------------------------------------------------
# Local storage
# ---------------------------------------------------------------------------

def local_tile_file(output_folder, tile_number):
    """Where a tile is stored locally. Mirrors the server path, plus ".bin.gz".

    Example: tile 1234567 -> <output_folder>/x001/x234/567.bin.gz
    Each folder holds at most 1,000 files, which keeps folders fast to list.
    """
    return os.path.join(output_folder, tile_path(tile_number) + ".bin.gz")


def tile_exists(output_folder, tile_number):
    return os.path.exists(local_tile_file(output_folder, tile_number))


def _write_atomically(file_name, data, mode):
    """Write to a temporary file, then rename it into place.

    A rename is all-or-nothing, so a crash mid-write never leaves a half-written
    file that looks complete.
    """
    temporary_name = file_name + ".tmp"
    with open(temporary_name, mode) as f:
        f.write(data)
    os.replace(temporary_name, file_name)


def save_tile(output_folder, tile_number, data):
    """Save a tile gzip-compressed (about 2.4x smaller in our tests)."""
    file_name = local_tile_file(output_folder, tile_number)
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    # Level 6 gives nearly the same size as the maximum (9) and is much faster.
    _write_atomically(file_name, gzip.compress(data, compresslevel=6), "wb")
    return file_name


def read_saved_tile(output_folder, tile_number):
    """Read a saved tile back and return its uncompressed bytes."""
    with gzip.open(local_tile_file(output_folder, tile_number), "rb") as f:
        return f.read()


def fetch_tile_to_disk(log_url, tile_number, output_folder):
    """Download a tile and save it, unless it's already saved.

    Returns the number of bytes downloaded (0 if the tile was already on disk).
    Used by both backfill.py and stream.py.
    """
    if tile_exists(output_folder, tile_number):
        return 0
    data = download_tile(log_url, tile_number)
    save_tile(output_folder, tile_number, data)
    return len(data)


def load_progress(progress_file):
    """Return the saved progress as a dict, or None if there is none yet."""
    if not os.path.exists(progress_file):
        return None
    with open(progress_file) as f:
        return json.load(f)


def save_progress(progress_file, progress):
    folder = os.path.dirname(progress_file)
    if folder:
        os.makedirs(folder, exist_ok=True)
    progress["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    _write_atomically(progress_file, json.dumps(progress, indent=2), "w")
