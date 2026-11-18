#!/usr/bin/env python3
"""CDC 复制槽回收值班台 — 端到端自测(仅用标准库)。

覆盖场景:
  1. 创建演示槽 / 重复创建拒绝
  2. 续租 + 租约有效时回收被拒(续租先成功 -> 不得删除)
  3. 租约过期后回收成功; 旧续租被拒(回收先成功 -> 旧续租失败)
  4. 同名重建 -> 旧代续租不得影响新槽
  5. 模拟写入 -> 保留 WAL 字节增长; 其他写入不受回收影响
  6. 数据库正在使用槽(pg_recvlogical 真实消费) -> 回收被拒且不删除、可再续租
  7. 崩溃恢复: tombstone 遗留时回收能继续完成删除
  8. 服务重启后按数据库实况显示, 不谎报
环境变量:
  BASE_URL         默认 http://127.0.0.1:8000
  PGBIN            本地 PostgreSQL bin 目录(含 pg_recvlogical/psql)
  APP_RESTART_CMD  重启应用的命令(可选; 不设则跳过重启用例)
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
PGBIN = os.environ.get("PGBIN", os.path.expanduser("~/cdc-runtime/pg16/bin"))
PG = dict(host="127.0.0.1", port=os.environ.get("PGPORT", "5432"),
          user="postgres", db="appdb")
RESTART_CMD = os.environ.get("APP_RESTART_CMD")

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}


def wait_app(timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            code, j = req("GET", "/api/status")
            if code == 200 and j.get("db_ok"):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def status():
    code, j = req("GET", "/api/status")
    assert code == 200 and j.get("db_ok"), f"status 异常: {code} {j}"
    return j


def psql(sql):
    r = subprocess.run(
        [os.path.join(PGBIN, "psql"), "-h", PG["host"], "-p", PG["port"],
         "-U", PG["user"], "-d", PG["db"], "-X", "-c", sql],
        capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, f"psql 失败: {r.stderr}"
    return r.stdout


def main():
    print(f"== 目标 {BASE} ==")
    assert wait_app(), "应用未就绪"
    print("== 应用就绪 ==")

    # ---------- 1. 初始状态 ----------
    st = status()
    check("初始: 槽不存在", st["slot"]["exists"] is False, st)
    check("初始: 无租约", st["lease"] is None, st)

    # ---------- 2. 创建 ----------
    code, j = req("POST", "/api/slot/create")
    check("创建演示槽", code == 200 and j.get("ok") and j.get("generation") == 1, j)
    st = status()
    check("创建后: 槽存在", st["slot"]["exists"] is True, st)
    check("创建后: 槽空闲(active=false)", st["slot"]["active"] is False, st)
    check("创建后: 保留 WAL 为数值", isinstance(st["slot"]["retained_bytes"], int), st)
    check("创建后: 租约 active 且未过期",
          st["lease"] and st["lease"]["state"] == "active" and not st["lease"]["expired"], st)
    check("创建后: 租约约 5 秒", 3000 < st["lease"]["remaining_ms"] <= 5200, st["lease"])

    code, j = req("POST", "/api/slot/create")
    check("重复创建被拒(409)", code == 409 and j.get("error") == "slot_exists", j)

    # ---------- 3. 续租先成功 -> 回收不得删除 ----------
    code, j = req("POST", "/api/slot/renew", {"generation": 1})
    check("续租(代1)成功", code == 200 and j.get("ok"), j)
    check("续租后剩余约 5 秒", 4000 < j.get("remaining_ms", 0) <= 5200, j)
    code, j = req("POST", "/api/slot/reclaim")
    check("租约有效时回收被拒(409 lease_active)",
          code == 409 and j.get("error") == "lease_active", j)
    st = status()
    check("被拒后槽仍存在", st["slot"]["exists"] is True, st)

    code, j = req("POST", "/api/slot/renew", {"generation": 999})
    check("错误代次续租被拒(409 generation_mismatch)",
          code == 409 and j.get("reason") == "generation_mismatch", j)

    # ---------- 4. 过期后回收成功 -> 旧续租失败 ----------
    print("  … 等待租约过期(6s)")
    time.sleep(6)
    code, j = req("POST", "/api/slot/reclaim")
    check("过期后回收成功", code == 200 and j.get("ok") and j.get("reclaimed"), j)
    st = status()
    check("回收后槽不存在", st["slot"]["exists"] is False, st)
    check("回收后租约为 reclaimed", st["lease"] and st["lease"]["state"] == "reclaimed", st)
    code, j = req("POST", "/api/slot/renew", {"generation": 1})
    check("回收后旧续租被拒(409 lease_reclaimed)",
          code == 409 and j.get("reason") == "lease_reclaimed", j)

    # ---------- 5. 同名重建 -> 旧代不得影响新槽 ----------
    code, j = req("POST", "/api/slot/create")
    check("同名重建成功(代=2)", code == 200 and j.get("generation") == 2, j)
    code, j = req("POST", "/api/slot/renew", {"generation": 1})
    check("旧代续租被拒(409)", code == 409 and j.get("reason") == "generation_mismatch", j)
    code, j = req("POST", "/api/slot/renew", {"generation": 2})
    check("新代续租成功", code == 200 and j.get("ok"), j)
    st = status()
    check("新槽未被旧代操作影响(仍存在)", st["slot"]["exists"] is True, st)

    # ---------- 6. 模拟写入 -> 保留 WAL 增长 ----------
    code, j = req("POST", "/api/demo/write", {"rows": 200})
    check("模拟写入 200 行", code == 200 and j.get("inserted") == 200, j)
    r1 = status()["slot"]["retained_bytes"]
    code, j = req("POST", "/api/demo/write", {"rows": 500})
    check("模拟写入 500 行", code == 200 and j.get("inserted") == 500, j)
    r2 = status()["slot"]["retained_bytes"]
    check("保留 WAL 字节增长", r2 > r1, f"{r1} -> {r2}")

    # ---------- 7. 数据库正在使用槽 -> 回收失败且不删除 ----------
    print("  … 等待租约过期(6s)")
    time.sleep(6)
    recvlog = os.path.join(PGBIN, "pg_recvlogical")
    proc = subprocess.Popen(
        [recvlog, "-h", PG["host"], "-p", PG["port"], "-U", PG["user"],
         "-d", PG["db"], "-S", "cdc_demo_slot", "--start",
         "-o", "proto_version=1", "-o", "publication_names=cdc_pub",
         "-f", "/dev/null"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        active = False
        for _ in range(30):
            if status()["slot"].get("active"):
                active = True
                break
            time.sleep(0.5)
        check("消费者接入后槽 active=true", active, status()["slot"])
        code, j = req("POST", "/api/slot/reclaim")
        check("槽使用中回收被拒(409 slot_active)",
              code == 409 and j.get("error") == "slot_active", j)
        st = status()
        check("回收失败后槽仍存在", st["slot"]["exists"] is True, st)
        check("回收失败后租约回滚为 active(未谎报)",
              st["lease"] and st["lease"]["state"] == "active", st["lease"])
        code, j = req("POST", "/api/slot/renew", {"generation": 2})
        check("回收失败后消费者可再续租", code == 200 and j.get("ok"), j)
        code, j = req("POST", "/api/demo/write", {"rows": 50})
        check("回收失败后其他写入仍可执行", code == 200 and j.get("inserted") == 50, j)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    inactive = False
    for _ in range(40):
        if not status()["slot"].get("active"):
            inactive = True
            break
        time.sleep(0.5)
    check("消费者断开后槽 active=false", inactive, status()["slot"])

    print("  … 等待租约过期(6s)")
    time.sleep(6)
    code, j = req("POST", "/api/slot/reclaim")
    check("消费者断开后回收成功", code == 200 and j.get("reclaimed"), j)
    check("槽已删除", status()["slot"]["exists"] is False)

    # ---------- 8. 崩溃恢复: tombstone 遗留 -> 回收继续完成 ----------
    code, j = req("POST", "/api/slot/create")
    check("再次创建(代=3)", code == 200 and j.get("generation") == 3, j)
    psql("UPDATE cdc_slot_lease SET state='reclaimed'")  # 模拟崩溃遗留的 tombstone
    st = status()
    check("实况: tombstone 遗留且槽仍存在",
          st["lease"]["state"] == "reclaimed" and st["slot"]["exists"], st)
    code, j = req("POST", "/api/slot/reclaim")
    check("恢复路径回收成功", code == 200 and j.get("reclaimed"), j)
    check("恢复后槽不存在", status()["slot"]["exists"] is False)

    # ---------- 9. 服务重启: 按实况显示, 不丢不谎报 ----------
    code, j = req("POST", "/api/slot/create")
    check("重启前创建(代=4)", code == 200 and j.get("generation") == 4, j)
    if RESTART_CMD:
        r = subprocess.run(RESTART_CMD, shell=True, timeout=120)
        check("应用重启命令执行成功", r.returncode == 0, r)
        assert wait_app(), "重启后应用未就绪"
        st = status()
        check("重启后槽仍存在(实况)", st["slot"]["exists"] is True, st)
        check("重启后租约未丢(代=4, active)",
              st["lease"] and st["lease"]["generation"] == 4
              and st["lease"]["state"] == "active", st["lease"])
        print("  … 等待租约过期(6s)")
        time.sleep(6)
        code, j = req("POST", "/api/slot/reclaim")
        check("重启后回收仍可用", code == 200 and j.get("reclaimed"), j)
    else:
        print("  … 跳过重启用例(未设 APP_RESTART_CMD)")

    # ---------- 汇总 ----------
    st = status()
    check("终态: 槽不存在", st["slot"]["exists"] is False, st)
    print(f"\n== 结果: {PASS} 通过, {FAIL} 失败 ==")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
