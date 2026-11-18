# CDC 复制槽回收值班台

值班员用于回收**失联 CDC 消费者**占用的 PostgreSQL 逻辑复制槽的操作网页，避免 WAL 持续堆积。

- 数据库：PostgreSQL 16（`wal_level=logical`）
- 服务：FastAPI + psycopg 3（同步驱动），单页操作界面（原生 JS）
- 编排：Docker Compose（`db` + `app` 两个服务）

## 快速开始

```bash
docker compose up --build
# 网页: http://localhost:8000
```

网页上可执行：

| 操作 | 说明 |
|---|---|
| ① 创建演示槽 | 创建逻辑复制槽 `cdc_demo_slot`（pgoutput）并签发 5 秒租约 |
| ② 模拟写入 | 向 `demo_events` 写 100 行，产生 WAL（任何时候都可执行） |
| ③ 续租 | 模拟消费者心跳，租约延长 5 秒（携带 fencing 代次） |
| ④ 确认回收 | 仅当**租约过期**且**数据库确认槽未被使用**时才真正删除 |

界面实时显示：实际保留 WAL 字节数（`pg_current_wal_lsn() - restart_lsn`）、未确认字节数、
数据库是否正在使用该槽（`active`/`active_pid`）、租约状态/代次/倒计时。

## 安全设计（全部以数据库实况为准）

租约持久化在 `cdc_slot_lease` 表，`generation` 列是 **fencing token（代次）**：

1. **续租**是单条原子 `UPDATE ... WHERE slot_name=? AND generation=? AND state='active'`。
2. **回收在单个数据库事务内完成全部动作**：`SELECT ... FOR UPDATE` 锁租约行
   → 资格判定 → 带代次守卫的 tombstone（`state→'reclaimed'`）→ 查
   `pg_replication_slots` 实况 → 删槽前代次复核 → `pg_drop_replication_slot`
   → 删后复核 + 代次复核，全部成功才 `COMMIT`；任一步不满足即整体 `ROLLBACK`，
   tombstone 不会半落地，删除失败可按数据库实况安全重试。
3. **交错语义**由同一把行锁 + READ COMMITTED 保证：
   - 续租先成功（仍在有效期）→ 回收的资格判定不满足 → **拒绝删除**；
   - 回收 tombstone 先提交 → 旧续租的 `state='active'` 条件不满足 → **续租失败**。
4. **回收与同名重建的交错**（本次修复重点）：两者争抢 `cdc_slot_lease` 同一行锁。
   - 回收事务持锁期间，重建的租约 upsert 被阻塞，必须等回收提交后才能以
     `generation+1` 生效，因此**迟到的旧回收物理上看不到也碰不到新槽/新租约**；
   - 回收的删除动作严格绑定“取得回收资格时锁定的那一代”，删槽前后各做一次
     代次守卫，任何代次/状态漂移都拒绝删除并返回 `generation_changed`；
   - 若重建先拿到锁并提交了新一代 active 租约，旧回收恢复后按新代实况重新判定：
     新代未过期 → 拒绝（`lease_active`，不动新租约）；新代也过期 → 只回收新代自己，
     响应中的 `generation` 如实标注实际被回收的代次，**绝不谎报回收了新代**。
5. **不谎报**：槽 active、删除失败或代次漂移时事务回滚并如实返回；
   所有状态来自 `pg_replication_slots` / 租约表实况，服务无内存状态，**重启不丢**；
   崩溃遗留 tombstone（已标记 reclaimed 但槽未删）时，再次回收在同一单事务内
   沿恢复路径完成删除；对已不存在的槽重复回收是幂等的。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/status` | 槽/租约/WAL 实况；库不可达时 `db_ok:false` |
| POST | `/api/slot/create` | 建槽 + 签发新一代租约；已存在返回 409 |
| POST | `/api/slot/renew` | `{generation}` 续租；拒绝返回 409 + 原因 |
| POST | `/api/slot/reclaim` | 回收；返回逐步骤日志与最终实况 |
| POST | `/api/demo/write` | `{rows}` 模拟业务写入 |

## 模拟“数据库正在使用槽”

```bash
docker compose exec db pg_recvlogical -U postgres -d appdb \
  -S cdc_demo_slot --start -o proto_version=1 -o publication_names=cdc_pub -f /dev/null
```

此时 `active=true`，回收会被拒绝且槽保留；`Ctrl-C` 断开后即可回收。

## 自测

- `tests/selftest.py`（44 项断言：创建/续租/回收交错、同名重建、active 拒绝、
  崩溃恢复、服务重启、WAL 增长、失败后可写）；
- `tests/race_test.py`（续租/回收真并发压测，默认 25 轮，断言两者不可能同时成功）；
- `tests/interleave_test.py`（**回收/重建交错专项**）：
  - A. HTTP 黑盒：两个并发回收与同名重建同时发起，多轮断言新槽必存活、
    新代租约不被旧回收改动、任何响应都不谎报回收新代；
  - B. 数据库白盒：手工编排“旧回收持锁 tombstone → 重建 upsert 被阻塞 →
    旧回收提交 → 重建才以新一代生效”的精确时序，验证新槽/新租约完好。
  - 白盒部分需要 `psycopg`；黑盒仅用标准库。通过 `BASE_URL` 指向服务运行，
    `PG*` 环境变量用于白盒直连。

前两者仅依赖 Python 标准库，通过 `BASE_URL` 指向服务即可运行；
`APP_RESTART_CMD` 可提供应用重启命令以启用重启用例。

`scripts/dev-up.sh` / `scripts/app-restart.sh` 是在无 Docker 环境下复现同一拓扑
（相同环境变量、`wal_level=logical`）的本地编排脚本，用于沙箱内自测。

## 崩溃与重试语义补充

- 逻辑复制槽的**创建是非事务性的**（`pg_create_logical_replication_slot` 不会随
  ROLLBACK 撤销），因此“建槽 + 租约 upsert”无法放进单个原子事务。
  本服务采用“先 upsert 租约、再建槽”的顺序，把与回收的争用收敛到租约行锁。
- 若进程恰好在“租约已提交、物理槽未建成”之间崩溃，会留下“无槽的 active 租约”。
  下次调用创建接口时的**自愈前检**会识别该状态并复用同一代租约补建槽
  （不晋升代次、响应中带 `recovered` 说明）；若遗留租约已过期则正常晋升新一代。
- 回收事务内删槽失败会整体回滚（tombstone 不落库），对已不存在的槽重复回收幂等。

## 备注

- compose 中 `POSTGRES_HOST_AUTH_METHOD: trust` 仅为本地演示方便 `pg_recvlogical`，生产请移除。
- 单操作者模型：多实例部署时并发回收仍安全（tombstone 原子、删后复核、误报回滚），
  但租约签发假定消费者只有一个持有者（演示场景）。
