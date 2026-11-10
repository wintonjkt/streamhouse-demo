#!/usr/bin/env bash
# Starts the Banking Streamhouse demo backend and web page.
cd "$(dirname "$0")" || exit 1
echo "Open http://localhost:8000/  (user guide: http://localhost:8000/#guide)"
exec python3 -m uvicorn backend.main:app --port 8000
