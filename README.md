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
2. **回收**先在事务内 `SELECT ... FOR UPDATE` 并原子 tombstone（`state→'reclaimed'`），
   再查 `pg_replication_slots` 实况，最后 `pg_drop_replication_slot`，删后复核。
3. **交错语义**由行锁 + READ COMMITTED 重评估保证：
   - 续租先成功（仍在有效期）→ 回收的 tombstone 条件不满足 → **拒绝删除**；
   - 回收 tombstone 先提交 → 旧续租的 `state='active'` 条件不满足 → **续租失败**。
4. **同名重建**使 `generation +1`，旧代续租无法影响新槽。
5. **不谎报**：槽 active 或删除失败时按原样回滚 tombstone 并如实返回；
   所有状态来自 `pg_replication_slots` / 租约表实况，服务无内存状态，**重启不丢**；
   崩溃遗留 tombstone（已标记 reclaimed 但槽未删）时，再次回收沿恢复路径完成删除。

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

`tests/selftest.py`（44 项断言：交错、同名重建、active 拒绝、崩溃恢复、服务重启、
WAL 增长、失败后可写）与 `tests/race_test.py`（续租/回收真并发压测）。
两者仅依赖 Python 标准库，通过 `BASE_URL` 指向服务即可运行；
`APP_RESTART_CMD` 可提供应用重启命令以启用重启用例。

`scripts/dev-up.sh` / `scripts/app-restart.sh` 是在无 Docker 环境下复现同一拓扑
（相同环境变量、`wal_level=logical`）的本地编排脚本，用于沙箱内自测。

## 备注

- compose 中 `POSTGRES_HOST_AUTH_METHOD: trust` 仅为本地演示方便 `pg_recvlogical`，生产请移除。
- 单操作者模型：多实例部署时并发回收仍安全（tombstone 原子、删后复核、误报回滚），
  但租约签发假定消费者只有一个持有者（演示场景）。
