"""
fake_ct_log.py - A tiny local web server that imitates a static-ct-api CT log.

Lets you test backfill.py, stream.py, and parser.py without touching the real
Let's Encrypt log (no network, no rate limits, runs in seconds).

What it serves, same layout as the real log:
    /checkpoint            text file; line 2 = number of entries
    /tile/data/<path>      tiles of 256 entries with real (fake-CA) certificates
    anything else          403 Access Denied, like Amazon S3

Entry timestamps are spread evenly over the past --days days, so binary search
by time can be tested. With --growth, the log keeps growing while it runs,
which is what stream.py needs.

Usage (run from the repo root, in its own terminal):
    python tests/fake_ct_log.py                         # 2,000 tiles over 30 days, port 8000
    python tests/fake_ct_log.py --growth 50             # also add 50 entries per second
    python tests/fake_ct_log.py --fail-rate 0.1 --gzip  # 10% random 503 errors, gzip tiles

Then point a script at it:
    python ingest/backfill.py --log-url http://localhost:8000/ --days 1
"""

import argparse
import gzip
import hashlib
import random
import re
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

ENTRIES_PER_TILE = 256
SAMPLE_DOMAINS = [
    ["paypa1-login.com", "www.paypa1-login.com"],
    ["example.org"],
    ["a1b2c3d4.devices.example.net"],
    ["secure-chase-verify.xyz"],
    ["shop.example.com", "example.com"],
    ["rnicrosoft-account.top"],
    ["blog.example.io"],
    ["xn--pypal-4ve.com"],
]


def make_certificates():
    """Build a fake CA and a few leaf certificates signed by it."""
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                         x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Fake CA"),
                         x509.NameAttribute(NameOID.COMMON_NAME, "Fake Test CA R1")])
    ca_key_der = ca_key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    issuer_key_hash = hashlib.sha256(ca_key_der).digest()

    certificates = []
    now = datetime.now(timezone.utc)
    for number, domains in enumerate(SAMPLE_DOMAINS, start=1):
        leaf_key = ec.generate_private_key(ec.SECP256R1())
        builder = (x509.CertificateBuilder()
                   .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, domains[0])]))
                   .issuer_name(ca_name)
                   .public_key(leaf_key.public_key())
                   .serial_number(0x1000 + number)
                   .not_valid_before(now - timedelta(days=1))
                   .not_valid_after(now + timedelta(days=89))
                   .add_extension(x509.SubjectAlternativeName(
                       [x509.DNSName(name) for name in domains]), critical=False))
        certificates.append(builder.sign(ca_key, hashes.SHA256()))
    return certificates, issuer_key_hash


def length_prefixed(data, length_size):
    return len(data).to_bytes(length_size, "big") + data


class FakeLog:
    def __init__(self, tiles, days, growth):
        self.initial_entries = tiles * ENTRIES_PER_TILE + 100  # +100: last tile is partial
        self.growth = growth
        self.started = time.time()
        self.history_start_ms = int((self.started - days * 86400) * 1000)
        self.ms_per_entry = days * 86400 * 1000 / self.initial_entries
        self.certificates, self.issuer_key_hash = make_certificates()

    def size(self):
        return self.initial_entries + int((time.time() - self.started) * self.growth)

    def timestamp(self, index):
        if index < self.initial_entries:
            return int(self.history_start_ms + index * self.ms_per_entry)
        seconds_after_start = (index - self.initial_entries) / self.growth
        return int((self.started + seconds_after_start) * 1000)

    def entry(self, index):
        """One tile entry in the static-ct-api layout."""
        cert = self.certificates[index % len(self.certificates)]
        cert_der = cert.public_bytes(serialization.Encoding.DER)
        # leaf_index extension: type 0, 5-byte index (real Sunlight logs include this)
        extensions = b"\x00" + length_prefixed(index.to_bytes(5, "big"), 2)
        chain = length_prefixed(hashlib.sha256(b"fake-ca").digest(), 2)
        out = self.timestamp(index).to_bytes(8, "big")
        if index % 2 == 0:  # final certificate
            out += (0).to_bytes(2, "big")
            out += length_prefixed(cert_der, 3) + length_prefixed(extensions, 2)
        else:               # precertificate
            out += (1).to_bytes(2, "big")
            out += self.issuer_key_hash
            out += length_prefixed(cert.tbs_certificate_bytes, 3)
            out += length_prefixed(extensions, 2)
            out += length_prefixed(cert_der, 3)
        return out + chain

    def tile(self, tile_number):
        first = tile_number * ENTRIES_PER_TILE
        return b"".join(self.entry(i) for i in range(first, first + ENTRIES_PER_TILE))


def path_to_tile_number(path):
    """'x001/x234/567' -> 1234567, or None if the path isn't a valid full-tile path."""
    if not re.fullmatch(r"(x\d{3}/)*\d{3}", path):
        return None
    return int(path.replace("x", "").replace("/", ""))


def make_handler(fake_log, fail_rate, use_gzip):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if random.random() < fail_rate:
                return self.reply(503, b"Service Unavailable (simulated)")
            if self.path == "/checkpoint":
                size = fake_log.size()
                return self.reply(200, ("fake.log/test\n%d\nAAAA=\n" % size).encode())
            if self.path.startswith("/tile/data/"):
                tile_number = path_to_tile_number(self.path[len("/tile/data/"):])
                full_tiles = fake_log.size() // ENTRIES_PER_TILE
                if tile_number is not None and tile_number < full_tiles:
                    body = fake_log.tile(tile_number)
                    return self.reply(200, gzip.compress(body) if use_gzip else body)
            return self.reply(403, b"Access Denied")

        def reply(self, status, body):
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass  # keep the terminal quiet

    return Handler


def main():
    parser = argparse.ArgumentParser(description="Serve a fake static-ct-api log locally.")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--tiles", type=int, default=2000, help="full tiles at start (default 2000)")
    parser.add_argument("--days", type=float, default=30, help="days of history to spread them over")
    parser.add_argument("--growth", type=float, default=0, help="new entries per second (default 0)")
    parser.add_argument("--fail-rate", type=float, default=0, help="fraction of requests that get 503")
    parser.add_argument("--gzip", action="store_true", help="send tiles gzip-compressed")
    args = parser.parse_args()

    fake_log = FakeLog(args.tiles, args.days, args.growth)
    server = ThreadingHTTPServer(("localhost", args.port),
                                 make_handler(fake_log, args.fail_rate, args.gzip))
    print("Fake CT log at http://localhost:%d/ (%d entries, Ctrl+C to stop)"
          % (args.port, fake_log.size()))
    server.serve_forever()


if __name__ == "__main__":
    main()
