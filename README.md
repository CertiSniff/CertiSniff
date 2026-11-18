# CertiSniff

Early warning for phishing domains, using Certificate Transparency logs.

Every public TLS certificate is published in a Certificate Transparency (CT) log. CertiSniff downloads those logs at scale, extracts the domain names, and flags new domains that imitate real brands (for example `paypa1-login.com`) as soon as they get a certificate.

Final project for NYU CS6513 Big Data, Fall 2026.

## Pipeline

```
CT log (Let's Encrypt Sycamore 2027h1)
   -> ingest/backfill.py + ingest/stream.py   download tiles            [done]
   -> GCS bucket raw/                          gzip-compressed tiles     [next]
   -> parser (Spark on Dataproc)               tiles -> rows             [to do]
   -> GCS bucket parsed/                       Parquet, one folder per date
   -> detection (Spark)                        flag lookalike domains
   -> GCS bucket results/                      flagged domains, aggregates
   -> charts / dashboard
```

The download scripts currently save to a local folder. Writing straight to Google Cloud Storage comes next.

## Repository layout

```
ingest/
  ct_log.py        shared helpers: tile paths, downloads with retries, binary search, saving
  backfill.py      download the past N days of the log (resumable)
  stream.py        follow the log live, downloading new tiles every few minutes
tests/
  fake_ct_log.py   local fake CT log, for testing without the internet
requirements.txt
```

## Setup

Requires Python 3.10 or newer (tested with 3.13).

```bash
git clone <repo-url>
cd certisniff
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Run every command from the repo root (the `certisniff/` folder).

## Downloading CT data

### Quick test (about 15 minutes of the log)

```bash
python ingest/backfill.py --days 0.01 --max-tiles 5   # find the range, download 5 tiles
python ingest/backfill.py --days 0.01 --max-tiles 5   # resumes: downloads the next 5
rm -rf data                                           # clean up before a real run
```

### Backfill

```bash
python ingest/backfill.py              # past 5 days (default)
python ingest/backfill.py --days 20    # past 20 days
```

- On the first run it picks the range and saves it in `data/backfill_progress.json`. It records progress after every tile.
- If it crashes or you press Ctrl+C, run the same command again and it continues where it stopped.
- To start over with a different range, delete `data/backfill_progress.json`.
- Run multi-day backfills on the GCP VM, not a laptop. 5 days is about 80 GB.

### Streaming

```bash
python ingest/stream.py                # check for new tiles every 5 minutes, forever
python ingest/stream.py --once         # one round, then exit
```

- On its first run it starts at the tile right after backfill's last tile, so there is no gap. Without a backfill, it starts at the log's newest tile.
- It saves progress in `data/stream_progress.json` and resumes after a crash or restart.
- If the network fails, it logs the error and tries again next round.

Run any script with `--help` to see all options.

### Where files go

```
data/
  raw/x005/x024/550.bin.gz     one tile (256 log entries), gzip-compressed
  backfill_progress.json       backfill's range and next tile
  stream_progress.json         stream's next tile
```

Tiles are stored under the same path they have on the log server, so each folder holds at most 1,000 files.

### How backfill finds the tile from N days ago

Tile names don't contain dates, but every entry starts with an 8-byte timestamp, and timestamps increase with the tile number. So the tiles are sorted by time, and `backfill.py` binary-searches them: look at the middle tile's first timestamp, keep the half that contains the target time, and repeat. It takes about 20 tile downloads to find the exact tile among millions. On the real log, the tile it found starts 1 second before the target time.

## Testing without the internet

`tests/fake_ct_log.py` runs a local web server that imitates a CT log: the same file layout, real-format certificates, and S3-style 403 errors for missing files. It can also grow over time, fail some requests on purpose, and gzip its tiles.

```bash
# Terminal 1: a fake log that grows ~1 tile/s and fails 5% of requests
python tests/fake_ct_log.py --growth 300 --fail-rate 0.05 --gzip

# Terminal 2
python ingest/backfill.py --log-url http://localhost:8000/ --days 1
python ingest/stream.py --log-url http://localhost:8000/ --interval 10
```

## Data volume (Sycamore 2027h1, measured November 2026)

| | Per day | 5 days | 20 days |
|---|---|---|---|
| Tiles (256 entries each) | ~100,000 | ~510,000 | ~2,000,000 |
| Log entries | ~25 million | ~130 million | ~520 million |
| Storage (gzipped, 2.4x) | ~16 GB | ~80 GB | ~325 GB |

These numbers come from 15-minute samples, and traffic changes during the day.

## Data source

- Log: Let's Encrypt Sycamore 2027h1, `https://mon.sycamore.ct.letsencrypt.org/2027h1/`, which follows the [Static CT API](https://c2sp.org/static-ct-api).
- The scripts send a User-Agent that identifies the project and pause between requests (`--sleep`, default 0.2 s).
- Known gap: Let's Encrypt's 6-day certificates expire in 2026, so they go to the 2026h2 shard, which we don't read.
