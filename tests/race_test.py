#!/usr/bin/env python3
"""续租/回收真并发交错压测: 验证无论谁赢, 安全不变量都成立。

不变量:
  - 不允许出现 [续租成功 且 回收也成功](租约有效时槽被删);
  - 若续租成功 -> 槽必须仍然存在, 且回收必须被拒;
  - 若回收成功 -> 旧续租必须失败, 且槽必须不存在。
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
ROUNDS = int(os.environ.get("ROUNDS", "25"))


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


def status():
    return req("GET", "/api/status")[1]


def main():
    fails = 0
    for i in range(1, ROUNDS + 1):
        code, j = req("POST", "/api/slot/create")
        assert code == 200 and j.get("ok"), f"create 失败: {j}"
        gen = j["generation"]
        time.sleep(5.3)  # 等租约过期, 使回收具备竞争资格

        results = {}

        def do_renew():
            results["renew"] = req("POST", "/api/slot/renew", {"generation": gen})

        def do_reclaim():
            results["reclaim"] = req("POST", "/api/slot/reclaim")

        t1 = threading.Thread(target=do_renew)
        t2 = threading.Thread(target=do_reclaim)
        t1.start(); t2.start(); t1.join(); t2.join()

        renew_ok = results["renew"][0] == 200 and results["renew"][1].get("ok")
        reclaim_ok = results["reclaim"][0] == 200 and results["reclaim"][1].get("ok")
        st = status()
        exists = st["slot"]["exists"]

        ok = True
        if renew_ok and reclaim_ok:
            ok = False  # 租约有效时槽被删 —— 绝不允许
        if renew_ok and not exists:
            ok = False
        if reclaim_ok and exists:
            ok = False
        outcome = ("renew胜" if renew_ok else "") + ("reclaim胜" if reclaim_ok else "") or "双方皆拒"
        print(f"  轮次 {i:2d}: renew_ok={renew_ok} reclaim_ok={reclaim_ok} "
              f"slot_exists={exists}  [{outcome}]  {'OK' if ok else 'VIOLATION'}")
        if not ok:
            fails += 1

        # 清理: 若槽还在(续租赢了), 等过期后回收
        if exists:
            time.sleep(5.3)
            code, j = req("POST", "/api/slot/reclaim")
            assert code == 200 and j.get("reclaimed"), f"清理回收失败: {j}"

    print(f"\n== 并发交错: {ROUNDS - fails}/{ROUNDS} 轮满足不变量 ==")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
