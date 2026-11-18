"""
stream.py - Follow a CT log live: every few minutes, download the tiles added since last time.

How it works:
  1. Decides where to start (first run only):
       - right after backfill's last tile, if backfill's progress file exists,
         so there is no gap between the two scripts;
       - otherwise at the log's newest full tile (only new data from now on).
  2. Each round: reads the checkpoint, downloads every new full tile, and
     saves progress after each tile (same folder layout as backfill.py).
  3. Waits --interval seconds, then starts the next round. Runs until you stop it.

If it crashes, is stopped, or the network drops, run the same command again:
it continues from the saved progress and catches up on anything it missed.

Usage:
  python ingest/stream.py --once            # test: one round, then exit
  python ingest/stream.py                   # run forever, checking every 5 minutes
  python ingest/stream.py --interval 60     # check every minute
  python ingest/stream.py --help            # all options

Sycamore 2027h1 adds about 70 tiles per minute (Nov 2026), so 5 minutes of
data is ~350 tiles, ~1.5 minutes of downloading at ~4 tiles/s.

To start over, delete the progress file (default: data/stream_progress.json).
"""

import argparse
import logging
import sys
import time

import ct_log

log = logging.getLogger("certisniff")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Follow a CT log and download new tiles.")
    parser.add_argument("--log-url", default=ct_log.DEFAULT_LOG_URL,
                        help="monitoring URL of the log, ending in / (default: Sycamore 2027h1)")
    parser.add_argument("--output", default="data/raw",
                        help="where to save tiles: a folder or a bucket path like gs://my-bucket/raw "
                             "(default: data/raw)")
    parser.add_argument("--progress", default="data/stream_progress.json",
                        help="this script's progress file (default: data/stream_progress.json)")
    parser.add_argument("--backfill-progress", default="data/backfill_progress.json",
                        help="backfill's progress file, used to pick the first tile on the first run")
    parser.add_argument("--interval", type=float, default=300,
                        help="seconds to wait between rounds (default: 300 = 5 minutes)")
    parser.add_argument("--sleep", type=float, default=0.2,
                        help="seconds to wait between downloads (default: 0.2)")
    parser.add_argument("--once", action="store_true",
                        help="do one round and exit (for testing)")
    return parser.parse_args()


def choose_first_tile(args):
    """First run only: return the tile number to start from."""
    backfill = ct_log.load_progress(args.backfill_progress)
    if backfill is not None and backfill["log_url"] == args.log_url:
        first_tile = backfill["end_tile"] + 1
        log.info("Starting right after backfill's last tile (%d): tile %d",
                 backfill["end_tile"], first_tile)
        return first_tile

    newest = ct_log.last_full_tile(ct_log.get_log_size(args.log_url))
    log.info("No backfill progress found; starting at the newest full tile: %d", newest)
    return newest


def run_round(progress, args):
    """Download every full tile from progress["next_tile"] up to the newest one."""
    newest = ct_log.last_full_tile(ct_log.get_log_size(args.log_url))
    first = progress["next_tile"]
    if first > newest:
        log.info("Up to date (newest full tile is %d). Nothing new yet.", newest)
        return

    log.info("New tiles %d to %d (%d tiles)", first, newest, newest - first + 1)
    round_started = time.time()
    bytes_this_round = 0

    for tile_number in range(first, newest + 1):
        downloaded = ct_log.fetch_and_save_tile(args.log_url, tile_number, args.output)
        if downloaded:
            bytes_this_round += downloaded
            time.sleep(args.sleep)
        progress["next_tile"] = tile_number + 1
        ct_log.save_progress(args.progress, progress)

    seconds = time.time() - round_started
    log.info("Round done: tiles %d to %d, %.1f MB downloaded in %.0fs",
             first, newest, bytes_this_round / 1e6, seconds)


def run(args):
    progress = ct_log.load_progress(args.progress)
    if progress is None:
        progress = {"log_url": args.log_url, "next_tile": choose_first_tile(args)}
        ct_log.save_progress(args.progress, progress)
    elif progress["log_url"] != args.log_url:
        sys.exit("The progress file " + args.progress + " is for a different log ("
                 + progress["log_url"] + "). Use --progress to pick another file.")
    else:
        log.info("Resuming at tile %d", progress["next_tile"])

    while True:
        try:
            run_round(progress, args)
        except (ct_log.NotFoundError, RuntimeError) as error:
            # Network trouble that outlasted the retries. Don't crash: progress
            # is saved, so the next round picks up where this one stopped.
            log.error("Round failed (%s). Trying again next round.", error)

        if args.once:
            return
        log.info("Next check in %.0f seconds", args.interval)
        time.sleep(args.interval)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_arguments()
    try:
        run(args)
    except KeyboardInterrupt:
        log.info("Stopped by you. Progress is saved; run the same command to resume.")
        sys.exit(1)


if __name__ == "__main__":
    main()
