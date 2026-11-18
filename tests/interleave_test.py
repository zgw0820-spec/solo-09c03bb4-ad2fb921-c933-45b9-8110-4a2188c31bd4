#!/usr/bin/env python3
"""回收/重建交错的确定性复现测试(题目场景)。

场景: 旧租约过期后, 回收请求 R1 已取得回收资格(锁定旧代并 tombstone),
此时值班员同名重建 -> 新槽 + 新一代 active 租约; R1 恢复执行。

修复前(跨连接分步回收): R1 用新连接按槽名 pg_drop_replication_slot,
会把重建出的新槽删掉, 且返回"回收成功", 谎报回收了新代。

修复后(单事务回收 + 代次守卫): R1 始终持有旧代租约行锁, 重建的 upsert
被阻塞到 R1 提交之后; R1 只能删除自己锁定的旧代对应槽, 新槽/新租约不受影响。

本测试分两层:
  A. HTTP 黑盒: 两个并发回收 + 一个重建, 多轮压测, 断言安全不变量;
  B. 数据库白盒(可选, 需 psycopg 直连): 用两个真实事务手工编排精确交错,
     100% 复现"R1 持锁期间重建被阻塞"的时序, 断言新槽存活、代次不被谎报。

仅依赖标准库 + psycopg(白盒部分)。环境变量:
  BASE_URL   默认 http://127.0.0.1:8000
  PG*        直连数据库参数(白盒)
  ROUNDS     黑盒轮次, 默认 12
"""
import json
import os
import threading
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
ROUNDS = int(os.environ.get("ROUNDS", "12"))
PG = dict(
    host=os.environ.get("PGHOST", "127.0.0.1"),
    port=int(os.environ.get("PGPORT", "5432")),
    dbname=os.environ.get("PGDATABASE", "appdb"),
    user=os.environ.get("PGUSER", "postgres"),
    password=os.environ.get("PGPASSWORD", "postgres"),
)
SLOT = os.environ.get("SLOT_NAME", "cdc_demo_slot")
LEASE_SECONDS = int(os.environ.get("LEASE_SECONDS", "5"))


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}


def status():
    return req("GET", "/api/status")[1]


def cleanup():
    """保证起始干净: 槽存在则等过期后回收。"""
    st = status()
    if st["slot"]["exists"]:
        if st["lease"] and st["lease"]["state"] == "active" and not st["lease"]["expired"]:
            time.sleep(LEASE_SECONDS + 0.5)
        code, j = req("POST", "/api/slot/reclaim")
        assert j.get("reclaimed"), f"清理失败: {j}"


# ------------------------------------------------------- A. HTTP 黑盒压测 ----

def blackbox():
    print("== A. HTTP 黑盒: 并发回收 x2 与同名重建交错 ==")
    violations = 0
    for i in range(1, ROUNDS + 1):
        cleanup()
        code, j = req("POST", "/api/slot/create")
        assert code == 200 and j["ok"], j
        gen0 = j["generation"]
        time.sleep(LEASE_SECONDS + 0.5)  # 等旧租约过期, 回收具备资格

        results = {}

        def reclaim(tag):
            results[tag] = req("POST", "/api/slot/reclaim")

        def recreate():
            # 与两个回收并发发起, 随机制造时序交错
            time.sleep(0.02)
            results["create"] = req("POST", "/api/slot/create")

        threads = [
            threading.Thread(target=reclaim, args=("r1",)),
            threading.Thread(target=reclaim, args=("r2",)),
            threading.Thread(target=recreate),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        st = status()
        slot_exists = st["slot"]["exists"]
        lease = st["lease"]
        r1, r2 = results["r1"][1], results["r2"][1]
        cr = results["create"][1]
        create_ok = results["create"][0] == 200 and cr.get("ok")

        ok = True
        problems = []

        # 不变量 1: 任何声称回收成功的响应, 其 generation 必须与当时被回收的代一致,
        # 且成功回收后槽确实不存在(不能谎报)。
        for tag, rj in (("r1", r1), ("r2", r2)):
            if rj.get("reclaimed"):
                if "generation" not in rj:
                    ok = False
                    problems.append(f"{tag} 回收成功但未返回 generation")
                if rj["status"]["slot"]["exists"]:
                    ok = False
                    problems.append(f"{tag} 声称回收但实况槽仍存在")

        # 不变量 2: 若重建成功(新槽存在), 则新槽必须存活, 且新代租约 active。
        if create_ok:
            if not slot_exists:
                ok = False
                problems.append("重建成功但新槽被(迟到的旧回收)删除")
            if not lease or lease["generation"] != cr["generation"] \
                    or lease["state"] != "active":
                ok = False
                problems.append(f"重建后新代租约被改动: {lease}")
            # 新代编号必须严格大于旧代
            if cr["generation"] <= gen0:
                ok = False
                problems.append(f"重建代次未递增: {gen0} -> {cr['generation']}")
            # 任何回收响应都不能声称回收了新代(新代 active 租约仍在)
            for tag, rj in (("r1", r1), ("r2", r2)):
                if rj.get("reclaimed") and rj.get("generation") == cr["generation"]:
                    ok = False
                    problems.append(f"{tag} 谎报回收了重建后的新代 {cr['generation']}")

        # 不变量 3: 不允许"槽不存在却存在 active 租约"的脑裂状态。
        if not slot_exists and lease and lease["state"] == "active":
            ok = False
            problems.append("脑裂: 槽不存在但租约 active")

        print(f"  轮次 {i:2d}: create_ok={create_ok} slot_exists={slot_exists} "
              f"r1={r1.get('reclaimed')}/g{r1.get('generation')} "
              f"r2={r2.get('reclaimed')}/g{r2.get('generation')} "
              f"lease_g={lease and lease['generation']}/{lease and lease['state']} "
              f"{'OK' if ok else 'VIOLATION: ' + '; '.join(problems)}")
        if not ok:
            violations += 1

    print(f"== A 结果: {ROUNDS - violations}/{ROUNDS} 轮满足不变量 ==\n")
    return violations


# ------------------------------------------------- B. 数据库白盒确定性交错 ----

def whitebox():
    print("== B. 白盒: 手工编排 R1 持锁 -> 重建阻塞 -> R1 提交 的精确交错 ==")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:
        print("  跳过: 未安装 psycopg")
        return 0

    def conn(autocommit=False):
        c = psycopg.connect(**PG, autocommit=autocommit, connect_timeout=5,
                            row_factory=dict_row)
        # 统一用显式游标, 不依赖不同 psycopg 版本 Connection.execute 的返回类型
        c.exec_ = lambda q, p=None: c.cursor().execute(q, p)
        return c

    fails = 0

    def check(name, cond, detail=""):
        nonlocal fails
        print(("  PASS  " if cond else "  FAIL  ") + name + ("" if cond else f"  {detail}"))
        if not cond:
            fails += 1

    cleanup()
    code, j = req("POST", "/api/slot/create")
    assert code == 200 and j["ok"], j
    old_gen = j["generation"]
    time.sleep(LEASE_SECONDS + 0.5)  # 旧租约过期

    # R1: 模拟回收事务的前半段 —— 锁行 + tombstone, 但先不提交(复现"恢复执行前"窗口)
    r1 = conn()
    cur = r1.exec_("SELECT generation, state, (expires_at <= clock_timestamp()) AS expired "
               "FROM cdc_slot_lease WHERE slot_name=%(n)s FOR UPDATE", {"n": SLOT})
    locked = cur.fetchone()
    check("R1 锁定旧代且已过期", locked["generation"] == old_gen and locked["expired"], locked)
    r1.exec_("UPDATE cdc_slot_lease SET state='reclaimed', updated_at=clock_timestamp() "
               "WHERE slot_name=%(n)s AND generation=%(g)s",
               {"n": SLOT, "g": old_gen})

    # 重建连接: 租约 upsert 必须被 R1 的行锁阻塞(不能绕过锁产生新一代)
    creator = conn(autocommit=True)
    upsert_sql = """
    INSERT INTO cdc_slot_lease (slot_name, generation, state, expires_at, updated_at)
    VALUES (%(n)s, 1, 'active', clock_timestamp() + make_interval(secs=>%(s)s), clock_timestamp())
    ON CONFLICT (slot_name) DO UPDATE
    SET generation = cdc_slot_lease.generation + 1, state='active',
        expires_at = clock_timestamp() + make_interval(secs=>%(s)s),
        updated_at = clock_timestamp()
    RETURNING generation
    """
    done = {}

    def do_upsert():
        done["row"] = creator.exec_(upsert_sql, {"n": SLOT, "s": LEASE_SECONDS}).fetchone()

    th = threading.Thread(target=do_upsert)
    th.start()
    time.sleep(1.5)
    check("R1 持锁期间重建 upsert 被阻塞", th.is_alive())

    # R1 在锁内复核: 代次/状态仍是旧代 tombstone —— 守卫通过, 删旧槽并提交
    guard = r1.exec_("SELECT generation, state FROM cdc_slot_lease "
                       "WHERE slot_name=%(n)s FOR UPDATE", {"n": SLOT}).fetchone()
    check("R1 删槽前代次守卫仍指向旧代 tombstone",
          guard["generation"] == old_gen and guard["state"] == "reclaimed", guard)
    slot_before = r1.exec_("SELECT slot_name FROM pg_replication_slots WHERE slot_name=%(n)s",
                             {"n": SLOT}).fetchone()
    check("R1 事务内看到的仍是旧槽", slot_before is not None)
    r1.exec_("SELECT pg_drop_replication_slot(%(n)s)", {"n": SLOT})
    r1.commit()  # 提交后重建的 upsert 才得以继续

    th.join(timeout=10)
    check("R1 提交后重建 upsert 解除阻塞", not th.is_alive() and done.get("row"))
    new_gen = done["row"]["generation"]
    check("重建代次严格递增", new_gen == old_gen + 1, done["row"])

    # 重建物理槽(与 api_create 第 2 步相同)
    creator.exec_("SELECT pg_create_logical_replication_slot(%(n)s, 'pgoutput')",
                    {"n": SLOT})

    # 最终实况: 新槽存活, 新代 active 租约完好
    with conn(autocommit=True) as c:
        slot = c.exec_("SELECT slot_name, active FROM pg_replication_slots "
                         "WHERE slot_name=%(n)s", {"n": SLOT}).fetchone()
        lease = c.exec_("SELECT generation, state FROM cdc_slot_lease "
                          "WHERE slot_name=%(n)s", {"n": SLOT}).fetchone()
    check("新槽存活(未被迟到的旧回收删除)", slot is not None and slot["active"] is False, slot)
    check("新代租约 active 且未被旧回收改动",
          lease["generation"] == new_gen and lease["state"] == "active", lease)

    # 迟到的 R1 若此刻再通过 HTTP 回收: 新代未过期 -> 必须拒绝, 不删新槽
    code, j = req("POST", "/api/slot/reclaim")
    check("迟到旧回收对新代 active 租约返回 lease_active",
          code == 409 and j.get("error") == "lease_active", j)
    st = status()
    check("被拒后新槽仍然存活", st["slot"]["exists"] is True, st["slot"])
    check("被拒后新代租约仍 active",
          st["lease"]["generation"] == new_gen and st["lease"]["state"] == "active", st["lease"])

    r1.close()
    creator.close()
    print(f"== B 结果: {fails} 项失败 ==\n")
    return fails


def main():
    v = blackbox()
    v += whitebox()
    print(("全部通过 ✅" if v == 0 else f"存在 {v} 项违规 ❌"))
    return 1 if v else 0


if __name__ == "__main__":
    raise SystemExit(main())
