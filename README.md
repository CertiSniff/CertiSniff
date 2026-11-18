# CertiSniff

Early warning for phishing domains, using Certificate Transparency logs.

Every public TLS certificate is published in a Certificate Transparency (CT) log. CertiSniff downloads those logs at scale, extracts the domain names, and flags new domains that imitate real brands (for example `paypa1-login.com`) as soon as they get a certificate.

Final project for NYU CS6513 Big Data, Fall 2026.

## Pipeline

```
CT log (Let's Encrypt Sycamore 2027h1)
   -> ingest/backfill.py + ingest/stream.py   download tiles            [done]
   -> GCS bucket raw/                          gzip-compressed tiles     [done]
   -> parser (Spark on Dataproc)               tiles -> rows             [to do]
   -> GCS bucket parsed/                       Parquet, one folder per date
   -> detection (Spark)                        flag lookalike domains
   -> GCS bucket results/                      flagged domains, aggregates
   -> charts / dashboard
```

The download scripts save to a local folder for testing, or straight to a Google Cloud Storage bucket with `--output gs://<bucket>/raw`.

## Repository layout

```
ingest/
  ct_log.py        shared helpers: tile paths, downloads with retries, binary search, saving
  backfill.py      download the past N days of the log (resumable)
  stream.py        follow the log live, downloading new tiles every few minutes
deploy/
  install.sh       installs backfill and stream as systemd services on the VM
  *.service        the two service definitions
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
data/raw/x005/x024/550.bin.gz      one tile (256 log entries), gzip-compressed
  or gs://<bucket>/raw/x005/x024/550.bin.gz with --output gs://<bucket>/raw
data/backfill_progress.json        backfill's range and next tile (always local)
data/stream_progress.json          stream's next tile (always local)
```

Tiles are stored under the same path they have on the log server, so each folder holds at most 1,000 files.

### How backfill finds the tile from N days ago

Tile names don't contain dates, but every entry starts with an 8-byte timestamp, and timestamps increase with the tile number. So the tiles are sorted by time, and `backfill.py` binary-searches them: look at the middle tile's first timestamp, keep the half that contains the target time, and repeat. It takes about 20 tile downloads to find the exact tile among millions. On the real log, the tile it found starts 1 second before the target time.

## Running on GCP

The download scripts run on a small VM and write to a GCS bucket in the same region (us-east1). Run steps 1–4 in [Cloud Shell](https://shell.cloud.google.com) (a free browser terminal with `gcloud` installed), or anywhere with the gcloud CLI.

**Costs:** the bucket is about $0.02 per GB per month plus $0.005 per 1,000 files written. One e2-micro VM in us-east1 with up to 30 GB of standard disk is covered by the Compute Engine free tier, which doesn't use the free-trial credits. An e2-small is not free (roughly $12–15 per month).

### 1. Project and APIs

```bash
gcloud config set project <PROJECT_ID>
gcloud services enable compute.googleapis.com storage.googleapis.com
```

### 2. Bucket

Bucket names are global, so pick a unique one.

```bash
BUCKET=certisniff-<unique-suffix>
gcloud storage buckets create gs://$BUCKET --location=us-east1 \
    --uniform-bucket-level-access --soft-delete-duration=0
```

`--soft-delete-duration=0` turns off the default 7-day soft delete, so deleted test files stop costing money right away.

### 3. Service account for the VM

The VM gets its own identity that can read and write files in this bucket and nothing else.

```bash
gcloud iam service-accounts create certisniff-vm --display-name="CertiSniff download VM"
SA=certisniff-vm@$(gcloud config get-value project).iam.gserviceaccount.com
gcloud storage buckets add-iam-policy-binding gs://$BUCKET \
    --member="serviceAccount:$SA" --role="roles/storage.objectAdmin"
```

### 4. VM

```bash
gcloud compute instances create certisniff-ingest \
    --zone=us-east1-b --machine-type=e2-micro \
    --image-family=debian-12 --image-project=debian-cloud \
    --boot-disk-size=20GB --boot-disk-type=pd-standard \
    --service-account=$SA --scopes=cloud-platform
gcloud compute ssh certisniff-ingest --zone=us-east1-b
```

### 5. Install on the VM

```bash
sudo apt update && sudo apt install -y git python3-venv
git clone <repo-url> certisniff && cd certisniff
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 6. Test with a small range before the real run

```bash
.venv/bin/python ingest/backfill.py --days 0.003 \
    --output gs://<bucket>/test-raw --progress data/test_progress.json
gcloud storage ls "gs://<bucket>/test-raw/**" | wc -l      # should match the tile count
gcloud storage rm -r gs://<bucket>/test-raw && rm data/test_progress.json
```

Check the `tiles/s` figure in the output. If an e2-micro is much slower than about 3 tiles/s, switch to e2-small: stop the VM, run `gcloud compute instances set-machine-type certisniff-ingest --zone=us-east1-b --machine-type=e2-small`, then start it again.

### 7. Start the services

```bash
sudo bash deploy/install.sh <bucket> 5            # bucket name, backfill days
sudo systemctl enable --now certisniff-backfill
journalctl -u certisniff-backfill -f         # wait for the "Plan: tiles ..." line, then Ctrl+C
sudo systemctl enable --now certisniff-stream
```

Start backfill first. Stream reads backfill's progress file to start right after its last tile, so starting stream too early leaves a small gap.

### Checking on it

```bash
systemctl status certisniff-backfill certisniff-stream
journalctl -u certisniff-stream -n 50           # last 50 log lines
gcloud storage du -s -h gs://<bucket>/raw       # total size in the bucket
```

To stop: `sudo systemctl stop certisniff-stream` (progress is saved). Stopping the VM (`gcloud compute instances stop certisniff-ingest --zone=us-east1-b`) stops the VM and IP charges; the disk is still billed.

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
