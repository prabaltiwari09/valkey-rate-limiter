#!/usr/bin/env bash
set -e

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip -q
pip install valkey

echo ""
echo "Setup complete. To run the demo:"
echo "  source .venv/bin/activate"
echo "  python rate_limiter_demo.py"
