#!/usr/bin/env bash
# 本地开发/自测编排: 用编译好的 PostgreSQL 16 模拟 compose 中的 db 服务,
# 用 uvicorn 模拟 compose 中的 app 服务(环境变量与 compose.yaml 一致)。
# 用法: scripts/dev-up.sh [--reset]
set -euo pipefail
RT="${CDC_RUNTIME:-$HOME/cdc-runtime}"
PGBIN="$RT/pg16/bin"
PGDATA="$RT/pgdata"
PGPORT="${PGPORT:-5432}"
APP_DIR="$(cd "$(dirname "$0")/../app" && pwd)"

if [[ "${1:-}" == "--reset" ]]; then
  "$PGBIN/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
  pkill -f 'uvicorn main:app' 2>/dev/null || true
  rm -rf "$PGDATA"
fi

if [[ ! -s "$PGDATA/PG_VERSION" ]]; then
  "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth=trust -E UTF8 --no-instructions >/dev/null
  {
    echo "wal_level = logical"
    echo "max_replication_slots = 10"
    echo "max_wal_senders = 10"
    echo "port = $PGPORT"
    echo "listen_addresses = 'localhost'"
  } >> "$PGDATA/postgresql.conf"
fi

"$PGBIN/pg_ctl" -D "$PGDATA" -l "$RT/pg.log" -w -t 60 start >/dev/null
"$PGBIN/createdb" -h 127.0.0.1 -p "$PGPORT" -U postgres appdb 2>/dev/null || true

pkill -f 'uvicorn main:app' 2>/dev/null || true
sleep 1
cd "$APP_DIR"
PGHOST=127.0.0.1 PGPORT="$PGPORT" PGDATABASE=appdb PGUSER=postgres PGPASSWORD=postgres \
SLOT_NAME=cdc_demo_slot LEASE_SECONDS=5 \
  setsid nohup python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 \
  > "$RT/app.log" 2>&1 &

for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8000/api/status >/dev/null 2>&1; then
    echo "dev-up OK: app=http://127.0.0.1:8000 db=127.0.0.1:$PGPORT"
    exit 0
  fi
  sleep 1
done
echo "应用未就绪, 日志: $RT/app.log" >&2
exit 1
