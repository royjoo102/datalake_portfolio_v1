#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec ./datalake_env/bin/python -m streamlit run dashboard/app.py \
  --server.address 127.0.0.1 --server.port "${DASHBOARD_PORT:-8501}" \
  --server.headless true --browser.gatherUsageStats false "$@"
