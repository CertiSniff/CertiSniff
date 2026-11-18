"""
backfill.py - Download the past N days (default 5) of a CT log as raw tiles.

How it works:
  1. Reads the log's checkpoint. The newest full tile is the END of the range.
  2. Binary-searches the log for the tile logged N days ago. That's the START.
  3. Downloads every tile from START to END into the output folder, saved
     gzip-compressed as <output>/x001/x234/567.bin.gz.
  4. After each tile, writes progress to a small JSON file. If the script
     crashes or you stop it with Ctrl+C, run the same command again and it
     continues where it stopped.

The range is fixed on the first run and saved in the progress file. Tiles added
to the log after that are stream.py's job; stream.py starts right after END.

Usage:
  python ingest/backfill.py --days 0.01     # small test: last ~15 minutes (~1,000 tiles)
  python ingest/backfill.py --days 0.01 --max-tiles 5
                                            # stop after 5 tiles; run again to resume
  python ingest/backfill.py                 # full run: 5 days, ~510,000 tiles, ~80 GB
  python ingest/backfill.py --help          # all options

Sizes measured on Sycamore 2027h1 in Nov 2026: ~100,000 tiles (~16 GB gzipped)
per day. Don't run multi-day backfills on a laptop; use the GCP VM.

To start over with a different range, delete the progress file.
"""

import argparse
import logging
import sys
import time

import ct_log

log = logging.getLogger("certisniff")

REPORT_EVERY_SECONDS = 10  # how often to print a progress line during downloads


def parse_arguments():
    parser = argparse.ArgumentParser(description="Download the past N days of CT log tiles.")
    parser.add_argument("--log-url", default=ct_log.DEFAULT_LOG_URL,
                        help="monitoring URL of the log, ending in / (default: Sycamore 2027h1)")
    parser.add_argument("--days", type=float, default=5,
                        help="how many days back to start (default: 5; decimals allowed)")
    parser.add_argument("--output", default="data/raw",
                        help="folder to save tiles in (default: data/raw)")
    parser.add_argument("--progress", default="data/backfill_progress.json",
                        help="progress file used to resume (default: data/backfill_progress.json)")
    parser.add_argument("--max-tiles", type=int, default=0,
                        help="stop after this many tiles in this run (default: 0 = no limit)")
    parser.add_argument("--sleep", type=float, default=0.2,
                        help="seconds to wait between downloads (default: 0.2)")
    return parser.parse_args()


def plan_range(args):
    """First run only: decide which tiles to download and return a new progress dict."""
    log_size = ct_log.get_log_size(args.log_url)
    end_tile = ct_log.last_full_tile(log_size)
    target_ms = int((time.time() - args.days * 24 * 3600) * 1000)

    log.info("Log size: %d entries, newest full tile: %d", log_size, end_tile)
    log.info("Looking for the tile logged at %s (%s days ago)",
             ct_log.ms_to_text(target_ms), args.days)

    start_tile = ct_log.find_tile_at_time(args.log_url, target_ms, end_tile, args.sleep)

    # Sanity check: show when the start tile begins, so you can compare it to the target.
    start_time = ct_log.first_timestamp(ct_log.download_tile(args.log_url, start_tile))
    log.info("Start tile %d begins at %s (target was %s)",
             start_tile, ct_log.ms_to_text(start_time), ct_log.ms_to_text(target_ms))

    tile_count = end_tile - start_tile + 1
    log.info("Plan: tiles %d to %d = %d tiles (~%d certificate entries)",
             start_tile, end_tile, tile_count, tile_count * ct_log.ENTRIES_PER_TILE)

    return {
        "log_url": args.log_url,
        "days": args.days,
        "target_time": ct_log.ms_to_text(target_ms),
        "start_tile": start_tile,
        "end_tile": end_tile,
        "next_tile": start_tile,  # the first tile not downloaded yet
        "finished": False,
    }


def download_range(progress, args):
    """Download tiles from progress["next_tile"] to progress["end_tile"]."""
    start, end = progress["start_tile"], progress["end_tile"]
    first_this_run = progress["next_tile"]
    total = end - start + 1
    run_started = time.time()
    last_report = run_started
    tiles_this_run = 0
    bytes_this_run = 0
    log.info("Downloading tiles %d to %d into %s (progress update every %ds)",
             first_this_run, end, args.output, REPORT_EVERY_SECONDS)

    for tile_number in range(first_this_run, end + 1):
        if args.max_tiles and tiles_this_run >= args.max_tiles:
            log.info("Reached --max-tiles %d. Run the same command again to continue.",
                     args.max_tiles)
            break

        # Skips tiles already on disk (e.g. downloaded just before a crash).
        downloaded = ct_log.fetch_tile_to_disk(args.log_url, tile_number, args.output)
        if downloaded:
            bytes_this_run += downloaded
            time.sleep(args.sleep)

        # The tile is safely on disk, so record that it's done.
        progress["next_tile"] = tile_number + 1
        ct_log.save_progress(args.progress, progress)
        tiles_this_run += 1

        # Report on a timer, so a long run never looks frozen.
        if time.time() - last_report >= REPORT_EVERY_SECONDS or tile_number == end:
            report_speed(tile_number, start, total, tiles_this_run, bytes_this_run, run_started)
            last_report = time.time()

    if progress["next_tile"] > end:
        progress["finished"] = True
        ct_log.save_progress(args.progress, progress)
        log.info("Backfill complete: tiles %d to %d are in %s", start, end, args.output)

    if tiles_this_run:
        log.info("This run: tiles %d to %d (%d tiles, %.1f MB downloaded)",
                 first_this_run, progress["next_tile"] - 1, tiles_this_run, bytes_this_run / 1e6)


def report_speed(tile_number, start, total, tiles_this_run, bytes_this_run, run_started):
    """Log how far along we are and roughly how long is left."""
    done = tile_number - start + 1
    elapsed = time.time() - run_started
    tiles_per_second = tiles_this_run / elapsed if elapsed > 0 else 0
    seconds_left = (total - done) / tiles_per_second if tiles_per_second else 0
    if seconds_left >= 3600:
        time_left = "%.1f h" % (seconds_left / 3600)
    elif seconds_left >= 60:
        time_left = "%.0f min" % (seconds_left / 60)
    else:
        time_left = "%.0f s" % seconds_left
    log.info("Progress: %d / %d tiles (%.1f%%), %.1f tiles/s, %.0f MB this run, ~%s left",
             done, total, 100 * done / total, tiles_per_second, bytes_this_run / 1e6, time_left)


def run(args):
    progress = ct_log.load_progress(args.progress)
    if progress is None:
        progress = plan_range(args)
        ct_log.save_progress(args.progress, progress)
    else:
        if progress["log_url"] != args.log_url:
            sys.exit("The progress file " + args.progress + " is for a different log ("
                     + progress["log_url"] + "). Use --progress to pick another file.")
        if progress["days"] != args.days:
            log.warning("Ignoring --days %s: the saved range (%s days) is used. "
                        "Delete %s to plan a new range.", args.days, progress["days"], args.progress)
        if progress["finished"]:
            log.info("Backfill already finished (tiles %d to %d). Nothing to do.",
                     progress["start_tile"], progress["end_tile"])
            return
        log.info("Resuming: next tile %d (range %d to %d, %d tiles left)",
                 progress["next_tile"], progress["start_tile"], progress["end_tile"],
                 progress["end_tile"] - progress["next_tile"] + 1)

    download_range(progress, args)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_arguments()
    try:
        run(args)
    except KeyboardInterrupt:
        # Progress is written after every tile, so nothing is lost. If you stop
        # during the first-run search, no range is saved yet and the next run
        # simply searches again.
        log.info("Stopped by you. Run the same command to resume.")
        sys.exit(1)


if __name__ == "__main__":
    main()
