#!/usr/bin/env bash
# 重启应用进程(模拟服务重启), 供自测脚本通过 APP_RESTART_CMD 调用。
set -euo pipefail
RT="${CDC_RUNTIME:-$HOME/cdc-runtime}"
APP_DIR="$(cd "$(dirname "$0")/../app" && pwd)"

pkill -f 'uvicorn main:app' 2>/dev/null || true
sleep 1
cd "$APP_DIR"
PGHOST=127.0.0.1 PGPORT="${PGPORT:-5432}" PGDATABASE=appdb PGUSER=postgres PGPASSWORD=postgres \
  SLOT_NAME=cdc_demo_slot LEASE_SECONDS=5 \
  setsid nohup python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 \
  >> "$RT/app.log" 2>&1 &

for i in $(seq 1 60); do
  curl -sf http://127.0.0.1:8000/api/status >/dev/null 2>&1 && exit 0
  sleep 1
done
echo "应用重启后未就绪" >&2
exit 1
