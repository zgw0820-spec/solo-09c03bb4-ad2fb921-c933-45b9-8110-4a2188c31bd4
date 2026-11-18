"""CDC 复制槽回收值班台 (FastAPI + PostgreSQL 16).

用途: 值班员回收失联 CDC 消费者占用的逻辑复制槽, 避免 WAL 持续堆积。

安全不变量(全部以数据库实况为准, 服务本身无内存状态):
1. 租约持久化在 cdc_slot_lease 表, generation 列是 fencing token(代次)。
2. 续租 = 单条原子 UPDATE, 仅当 (slot_name, generation, state='active') 全部匹配才生效。
3. 回收 = 先在事务内 SELECT ... FOR UPDATE 并原子 tombstone(state -> 'reclaimed'),
   再依据 pg_replication_slots 实况决定能否 pg_drop_replication_slot:
   - 租约仍在有效期内 -> 拒绝(续租先成功则不得删除);
   - tombstone 先提交 -> 之后的旧续租必然失败(回收先成功则旧续租失败);
   - 数据库显示槽 active -> 回滚 tombstone, 不删除;
   - 删除失败 -> 回滚 tombstone 并如实上报, 绝不谎报已回收。
4. 同名重建会使 generation +1, 旧代续租无法影响新槽。
5. 崩溃恢复: tombstone 已提交但槽未删(服务重启/崩溃)时, 再次回收会沿着
   已有的 'reclaimed' tombstone 继续完成删除, 状态始终来自数据库实况。
"""
from __future__ import annotations

import os
import re
import time
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

SLOT_NAME = os.environ.get("SLOT_NAME", "cdc_demo_slot")
LEASE_SECONDS = int(os.environ.get("LEASE_SECONDS", "5"))
SLOT_PLUGIN = os.environ.get("SLOT_PLUGIN", "pgoutput")
PUBLICATION_NAME = os.environ.get("PUBLICATION_NAME", "cdc_pub")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

if not re.fullmatch(r"[a-z_][a-z0-9_]*", PUBLICATION_NAME):
    raise SystemExit(f"非法 PUBLICATION_NAME: {PUBLICATION_NAME!r}")

PG = dict(
    host=os.environ.get("PGHOST", "127.0.0.1"),
    port=int(os.environ.get("PGPORT", "5432")),
    dbname=os.environ.get("PGDATABASE", "appdb"),
    user=os.environ.get("PGUSER", "postgres"),
    password=os.environ.get("PGPASSWORD", "postgres"),
)

# ---------------------------------------------------------------- SQL --------

SLOT_SQL = """
SELECT s.slot_name, s.plugin, s.slot_type, s.database, s.temporary,
       s.active, s.active_pid, s.catalog_xmin::text AS catalog_xmin,
       s.restart_lsn::text AS restart_lsn,
       s.confirmed_flush_lsn::text AS confirmed_flush_lsn,
       s.wal_status, s.safe_wal_size,
       pg_current_wal_lsn()::text AS current_wal_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint          AS retained_bytes,
       pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)::bigint AS unconfirmed_bytes
FROM pg_replication_slots AS s
WHERE s.slot_name = %(n)s
"""

LEASE_SQL = """
SELECT generation, state, expires_at, updated_at,
       (expires_at <= clock_timestamp()) AS expired,
       (extract(epoch FROM (expires_at - clock_timestamp())) * 1000)::bigint AS remaining_ms
FROM cdc_slot_lease
WHERE slot_name = %(n)s
"""

# 续租: 单条原子 UPDATE —— fencing token(generation) + 状态双重校验。
RENEW_SQL = """
UPDATE cdc_slot_lease
SET expires_at = clock_timestamp() + make_interval(secs => %(secs)s),
    updated_at  = clock_timestamp()
WHERE slot_name = %(n)s AND generation = %(g)s AND state = 'active'
RETURNING generation, expires_at,
          (extract(epoch FROM (expires_at - clock_timestamp())) * 1000)::bigint AS remaining_ms
"""

# 回收第 1 步: 事务内行锁 + 原子 tombstone(租约过期 或 崩溃遗留 tombstone 才允许)。
TOMBSTONE_LOCK_SQL = """
SELECT generation, state, expires_at,
       (expires_at <= clock_timestamp()) AS expired,
       (extract(epoch FROM (expires_at - clock_timestamp())) * 1000)::bigint AS remaining_ms
FROM cdc_slot_lease
WHERE slot_name = %(n)s
FOR UPDATE
"""

TOMBSTONE_SQL = """
UPDATE cdc_slot_lease
SET state = 'reclaimed', updated_at = clock_timestamp()
WHERE slot_name = %(n)s
"""

# 回收失败回滚: 仅当我们本次把 active 改成了 reclaimed 才回滚, 且必须同代。
REVERT_SQL = """
UPDATE cdc_slot_lease
SET state = 'active', expires_at = %(old_expires)s, updated_at = clock_timestamp()
WHERE slot_name = %(n)s AND generation = %(g)s AND state = 'reclaimed'
"""

# 创建: 同名重建时 generation 单调 +1, 旧代 fencing token 立即失效。
LEASE_UPSERT_SQL = """
INSERT INTO cdc_slot_lease (slot_name, generation, state, expires_at, updated_at)
VALUES (%(n)s, 1, 'active',
        clock_timestamp() + make_interval(secs => %(secs)s), clock_timestamp())
ON CONFLICT (slot_name) DO UPDATE
SET generation = cdc_slot_lease.generation + 1,
    state      = 'active',
    expires_at = clock_timestamp() + make_interval(secs => %(secs)s),
    updated_at = clock_timestamp()
RETURNING generation, expires_at
"""

WRITE_SQL = """
INSERT INTO demo_events (payload)
SELECT 'evt-' || g || '-' || md5(random()::text)
FROM generate_series(1, %(n)s) AS g
"""

DDL = [
    """
    CREATE TABLE IF NOT EXISTS cdc_slot_lease (
        slot_name  text PRIMARY KEY,
        generation bigint NOT NULL,
        state      text NOT NULL CHECK (state IN ('active', 'reclaimed')),
        expires_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS demo_events (
        id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        payload    text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
    """,
]

# ------------------------------------------------------------ helpers --------


def db_connect(*, autocommit: bool = False):
    return psycopg.connect(
        **PG, autocommit=autocommit, connect_timeout=5, row_factory=dict_row
    )


def fetch_slot() -> dict | None:
    """直接读 pg_replication_slots 实况(独立 autocommit 连接)。"""
    with db_connect(autocommit=True) as conn:
        return conn.execute(SLOT_SQL, {"n": SLOT_NAME}).fetchone()


def slot_view(slot: dict | None) -> dict:
    if not slot:
        return {"exists": False}
    return {"exists": True, **slot}


def lease_view(lease: dict | None) -> dict | None:
    if not lease:
        return None
    return {
        **lease,
        "expires_at": lease["expires_at"].isoformat(),
        "updated_at": lease["updated_at"].isoformat(),
    }


def build_status() -> dict:
    try:
        with db_connect(autocommit=True) as conn:
            slot = conn.execute(SLOT_SQL, {"n": SLOT_NAME}).fetchone()
            lease = conn.execute(LEASE_SQL, {"n": SLOT_NAME}).fetchone()
            server_time = conn.execute("SELECT clock_timestamp() AS t").fetchone()["t"]
            events = conn.execute("SELECT count(*) AS c FROM demo_events").fetchone()["c"]
    except Exception as exc:  # 数据库不可达也要如实展示
        return {
            "ok": False,
            "db_ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "slot_name": SLOT_NAME,
            "lease_seconds": LEASE_SECONDS,
        }
    return {
        "ok": True,
        "db_ok": True,
        "error": None,
        "slot_name": SLOT_NAME,
        "lease_seconds": LEASE_SECONDS,
        "server_time": server_time.isoformat(),
        "slot": slot_view(slot),
        "lease": lease_view(lease),
        "demo_events": events,
    }


def init_db() -> None:
    with db_connect(autocommit=True) as conn:
        for stmt in DDL:
            conn.execute(stmt)
        pub = conn.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s", (PUBLICATION_NAME,)
        ).fetchone()
        if not pub:
            conn.execute(f'CREATE PUBLICATION "{PUBLICATION_NAME}" FOR ALL TABLES')


@asynccontextmanager
async def lifespan(_app: FastAPI):
    deadline = time.monotonic() + 90
    delay = 1.0
    while True:
        try:
            init_db()
            print("[startup] 数据库初始化完成", flush=True)
            break
        except Exception as exc:
            if time.monotonic() > deadline:
                # 服务照常启动, /api/status 会如实显示数据库不可达
                print(f"[startup] 数据库初始化失败(不再重试): {exc!r}", flush=True)
                break
            print(f"[startup] 等待数据库就绪: {exc!r}", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 5)
    yield


app = FastAPI(title="CDC 复制槽回收值班台", lifespan=lifespan)


def jresp(payload: dict, status_code: int = 200) -> JSONResponse:
    return JSONResponse(payload, status_code=status_code)


def db_unavailable(exc: Exception) -> JSONResponse:
    return jresp(
        {"ok": False, "error": "db_unavailable",
         "detail": f"{type(exc).__name__}: {exc}"},
        status_code=503,
    )


# ------------------------------------------------------------ endpoints ------


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/status")
def api_status():
    return jresp(build_status())


@app.post("/api/slot/create")
def api_create():
    """创建演示槽并签发新一代租约。已存在则拒绝(不重复创建)。"""
    try:
        if fetch_slot():
            return jresp(
                {"ok": False, "error": "slot_exists",
                 "detail": f"复制槽 {SLOT_NAME} 已存在", "status": build_status()},
                status_code=409,
            )
    except Exception as exc:
        return db_unavailable(exc)

    # 1) 建槽(autocommit: 逻辑槽创建不允许在已有写入的事务里)
    try:
        with db_connect(autocommit=True) as conn:
            row = conn.execute(
                "SELECT slot_name, lsn::text AS lsn "
                "FROM pg_create_logical_replication_slot(%(n)s, %(p)s)",
                {"n": SLOT_NAME, "p": SLOT_PLUGIN},
            ).fetchone()
    except Exception as exc:
        # 并发创建等: 以数据库实况为准再判断一次
        try:
            if fetch_slot():
                return jresp(
                    {"ok": False, "error": "slot_exists",
                     "detail": "复制槽已存在(并发创建?)", "status": build_status()},
                    status_code=409,
                )
        except Exception as exc2:
            return db_unavailable(exc2)
        return jresp(
            {"ok": False, "error": "create_failed",
             "detail": f"{type(exc).__name__}: {exc}", "status": build_status()},
            status_code=500,
        )

    # 2) 签发租约(同名重建 -> generation +1)
    try:
        with db_connect() as conn:
            lease = conn.execute(
                LEASE_UPSERT_SQL, {"n": SLOT_NAME, "secs": LEASE_SECONDS}
            ).fetchone()
    except Exception as exc:
        # 租约写库失败: 尽力把刚建的槽删掉, 不留无租约的槽
        try:
            with db_connect(autocommit=True) as conn:
                conn.execute("SELECT pg_drop_replication_slot(%(n)s)", {"n": SLOT_NAME})
        except Exception:
            pass
        return jresp(
            {"ok": False, "error": "lease_failed",
             "detail": f"{type(exc).__name__}: {exc}", "status": build_status()},
            status_code=500,
        )

    return jresp(
        {
            "ok": True,
            "created": True,
            "slot_lsn": row["lsn"],
            "generation": lease["generation"],
            "expires_at": lease["expires_at"].isoformat(),
            "status": build_status(),
        }
    )


class RenewIn(BaseModel):
    generation: int


@app.post("/api/slot/renew")
def api_renew(body: RenewIn):
    """消费者续租: 仅当 (槽名, 代次, state='active') 匹配才延长 5 秒。"""
    try:
        with db_connect() as conn:
            row = conn.execute(
                RENEW_SQL,
                {"n": SLOT_NAME, "g": body.generation, "secs": LEASE_SECONDS},
            ).fetchone()
            if row is None:
                cur = conn.execute(LEASE_SQL, {"n": SLOT_NAME}).fetchone()
                if cur is None:
                    reason, detail = "no_lease", "租约不存在(槽未创建?)"
                elif cur["state"] == "reclaimed":
                    reason, detail = "lease_reclaimed", "租约已被回收, 旧续租无效"
                else:
                    reason, detail = (
                        "generation_mismatch",
                        f"代次不符: 当前代为 {cur['generation']}, 收到 {body.generation}",
                    )
                return jresp(
                    {"ok": False, "error": "renew_rejected", "reason": reason,
                     "detail": detail, "lease": lease_view(cur),
                     "status": build_status()},
                    status_code=409,
                )
    except Exception as exc:
        return db_unavailable(exc)

    return jresp(
        {"ok": True, "generation": row["generation"],
         "expires_at": row["expires_at"].isoformat(),
         "remaining_ms": row["remaining_ms"], "status": build_status()}
    )


@app.post("/api/slot/reclaim")
def api_reclaim():
    """确认回收: 租约过期 且 数据库确认槽未被使用 才真正删除。"""
    steps: list[dict] = []

    # -- 第 1 步: 事务内原子 tombstone(与续租的交错在此分出先后) -------------
    try:
        with db_connect() as conn:
            row = conn.execute(TOMBSTONE_LOCK_SQL, {"n": SLOT_NAME}).fetchone()
            if row is None:
                return jresp(
                    {"ok": False, "reclaimed": False, "error": "no_lease",
                     "detail": "租约记录不存在", "steps": steps,
                     "status": build_status()},
                    status_code=409,
                )
            old = dict(row)
            allowed = old["state"] == "reclaimed" or (
                old["state"] == "active" and old["expired"]
            )
            if not allowed:
                return jresp(
                    {"ok": False, "reclaimed": False, "error": "lease_active",
                     "detail": f"租约仍在有效期内(剩余约 {old['remaining_ms']} ms), 不得删除",
                     "steps": steps, "status": build_status()},
                    status_code=409,
                )
            conn.execute(TOMBSTONE_SQL, {"n": SLOT_NAME})
        steps.append({"step": "tombstone_lease", "ok": True,
                      "detail": f"租约代次 {old['generation']} 已标记 reclaimed(原状态 {old['state']})"})
    except Exception as exc:
        return db_unavailable(exc)

    transitioned = old["state"] == "active"  # 本次是否把 active 改成了 reclaimed

    def revert_tombstone() -> None:
        if not transitioned:
            return
        try:
            with db_connect() as conn:
                conn.execute(
                    REVERT_SQL,
                    {"n": SLOT_NAME, "g": old["generation"],
                     "old_expires": old["expires_at"]},
                )
            steps.append({"step": "revert_tombstone", "ok": True,
                          "detail": "已按原样回滚租约标记"})
        except Exception as exc:  # 回滚失败也要如实记录
            steps.append({"step": "revert_tombstone", "ok": False,
                          "detail": f"{type(exc).__name__}: {exc}"})

    # -- 第 2 步: 数据库实况: 槽是否存在 / 是否正在使用 ------------------------
    try:
        slot = fetch_slot()
    except Exception as exc:
        revert_tombstone()
        return db_unavailable(exc)

    if slot and slot["active"]:
        revert_tombstone()
        steps.append({"step": "check_active", "ok": False,
                      "detail": f"数据库正在使用该槽(active_pid={slot['active_pid']}), 不得删除"})
        return jresp(
            {"ok": False, "reclaimed": False, "error": "slot_active",
             "detail": "数据库正在使用该复制槽, 已取消回收", "steps": steps,
             "status": build_status()},
            status_code=409,
        )
    steps.append({"step": "check_active", "ok": True,
                  "detail": "数据库确认槽未被使用" if slot else "数据库中已不存在该槽"})

    # -- 第 3 步: 真正删除 -----------------------------------------------------
    if slot:
        try:
            with db_connect(autocommit=True) as conn:
                conn.execute("SELECT pg_drop_replication_slot(%(n)s)", {"n": SLOT_NAME})
            steps.append({"step": "drop_slot", "ok": True,
                          "detail": "pg_drop_replication_slot 已执行"})
        except Exception as exc:
            # 删除报错后先核实实况: 可能并发回收已完成
            try:
                still = fetch_slot()
            except Exception as exc2:
                revert_tombstone()
                return db_unavailable(exc2)
            if still is None:
                steps.append({"step": "drop_slot", "ok": True,
                              "detail": f"删除报错但复核确认槽已不存在(可能并发回收完成): {exc}"})
            else:
                revert_tombstone()
                steps.append({"step": "drop_slot", "ok": False,
                              "detail": f"{type(exc).__name__}: {exc}"})
                return jresp(
                    {"ok": False, "reclaimed": False, "error": "drop_failed",
                     "detail": f"删除复制槽失败: {exc}", "steps": steps,
                     "status": build_status()},
                    status_code=500,
                )

    # -- 第 4 步: 删除后复核, 按实况上报 ---------------------------------------
    try:
        final = fetch_slot()
    except Exception as exc:
        return db_unavailable(exc)
    if final is not None:
        revert_tombstone()
        steps.append({"step": "verify", "ok": False, "detail": "删除后槽仍存在"})
        return jresp(
            {"ok": False, "reclaimed": False, "error": "verify_failed",
             "detail": "删除后复核发现槽仍存在, 已回滚租约标记", "steps": steps,
             "status": build_status()},
            status_code=500,
        )
    steps.append({"step": "verify", "ok": True, "detail": "复核确认槽已不存在"})

    return jresp(
        {"ok": True, "reclaimed": True, "steps": steps, "status": build_status()}
    )


class WriteIn(BaseModel):
    rows: int = Field(default=100, ge=1, le=100000)


@app.post("/api/demo/write")
def api_write(body: WriteIn):
    """模拟业务写入(产生 WAL)。与槽状态无关, 任何时候都可执行。"""
    try:
        with db_connect() as conn:
            cur = conn.execute(WRITE_SQL, {"n": body.rows})
            inserted = cur.rowcount
            total = conn.execute("SELECT count(*) AS c FROM demo_events").fetchone()["c"]
            lsn = conn.execute("SELECT pg_current_wal_lsn()::text AS l").fetchone()["l"]
    except Exception as exc:
        return db_unavailable(exc)
    return jresp(
        {"ok": True, "inserted": inserted, "total": total,
         "current_wal_lsn": lsn, "status": build_status()}
    )
