#!/usr/bin/env bash
# install.sh - Install backfill.py and stream.py as systemd services on the download VM.
#
# systemd is Linux's service manager. Running the scripts as services means they
# keep going after you log out, restart automatically if they crash, and start
# again if the VM reboots.
#
# Run from the repo root on the VM, after creating .venv (see README):
#     sudo bash deploy/install.sh <bucket-name> [backfill-days]
# Example:
#     sudo bash deploy/install.sh certisniff-data-123 5
#
# What it does:
#   1. Writes the settings (bucket, backfill days) to /etc/default/certisniff
#   2. Copies the two .service files to /etc/systemd/system, filled in with
#      your username and this repo's folder
#   3. Reloads systemd. It does NOT start anything; the README says how.

set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "Run with sudo: sudo bash deploy/install.sh <bucket-name> [backfill-days]"
    exit 1
fi
if [ $# -lt 1 ]; then
    echo "Usage: sudo bash deploy/install.sh <bucket-name> [backfill-days]"
    exit 1
fi

BUCKET="${1#gs://}"              # accept "my-bucket" or "gs://my-bucket"
BUCKET="${BUCKET%/}"
DAYS="${2:-5}"
RUN_AS="${SUDO_USER:?Run this with sudo from your normal account, not as root}"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [ ! -x "$REPO_DIR/.venv/bin/python" ]; then
    echo "No virtual environment at $REPO_DIR/.venv. Create it first (see README)."
    exit 1
fi

cat > /etc/default/certisniff <<EOF
# Settings for the CertiSniff services (written by deploy/install.sh).
# After editing, restart the services: sudo systemctl restart certisniff-stream
BUCKET=$BUCKET
BACKFILL_DAYS=$DAYS
EOF

for service in certisniff-backfill certisniff-stream; do
    sed -e "s|@USER@|$RUN_AS|g" -e "s|@REPO_DIR@|$REPO_DIR|g" \
        "$REPO_DIR/deploy/$service.service" > "/etc/systemd/system/$service.service"
done
systemctl daemon-reload

echo "Installed for user $RUN_AS, repo $REPO_DIR, bucket gs://$BUCKET, backfill $DAYS days."
echo "Next: sudo systemctl start certisniff-backfill   (see README, 'Running on the VM')"
