#!/bin/sh
# Create a virtual environment and install deps for Kafka client tests.
# Usage: ./setup_venv.sh   (from repo root or from this directory)
# Then:  .venv/bin/pytest test_thorstream.py -v

set -e
cd "$(dirname "$0")"
if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
.venv/bin/pip install -r requirements.txt
echo "Done. Run: .venv/bin/pytest test_thorstream.py -v"
