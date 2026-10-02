# 两阶段提交（2PC）：协调者 + 多个独立 SQLite 参与者

带**崩溃/挂起注入**与**日志恢复**的 2PC 实现。参与者各自持有独立的
SQLite 文件；协调者用另一个 SQLite 文件记录事务日志。

## 文件

| 文件 | 作用 |
|------|------|
| `common.py` | 结构化日志、崩溃点（`CRASH`）/挂起点（`BLOCK`）注入、JSON-over-HTTP RPC |
| `participant.py` | 2PC 参与者：键值表、事务日志、键锁；投票持久化、commit/abort 幂等、自主恢复 |
| `coordinator.py` | 2PC 协调者：阶段一并发投票、决定先落盘再通知、重试幂等、重启重放、sweeper 兜底 |
| `client.py` | 命令行客户端：提交事务 / 查状态；committed=0, aborted=2, pending=3 |
| `test_2pc.py` | 11 个端到端测试（进程级崩溃、交错竞争事务、不变量核对） |

## 快速开始

```bash
# 三个参与者，各自独立 db 文件
python3 participant.py --name pa --db data/pa.db --port 17010 --coord http://127.0.0.1:17000
python3 participant.py --name pb --db data/pb.db --port 17011 --coord http://127.0.0.1:17000
python3 participant.py --name pc --db data/pc.db --port 17012 --coord http://127.0.0.1:17000

# 协调者
python3 coordinator.py --db data/coord.db --port 17000 \
    --participants pa:17010,pb:17011,pc:17012

# 客户端：每个 --put 是“参与者:键=值”
python3 client.py submit T1 --put pa:x=1 --put pa:y=2 --put pb:z=3
python3 client.py status T1
```

## 保证的语义

- **投票先落盘再应答**：参与者在一个 SQLite 事务（`synchronous=FULL`，
  WAL）里原子写入 `PREPARED` 记录和键锁，fsync 之后才回票。投票后
  任何时刻崩溃，重启后记录与锁都还在。
- **决定先落盘再通知**：协调者先把 `COMMITTED`/`ABORTED` fsync 落盘，
  然后才向任何参与者发 commit/abort。决定一旦落盘便不可更改。
- **准备后不猜测结论**：参与者处于 `PREPARED` 时若协调者不可达，只
  保持待决并继续持锁，靠后台 watcher 轮询协调者 `/decision/<tid>`，
  绝不自行提交或中止。协调者对未知事务也回 `UNKNOWN` 而非编造结果。
- **相同 tid 重试不重复执行**：tid 是幂等键。协调者发现 tid 已存在
  就走恢复/推进路径，忽略新的写入内容；参与者 commit/abort 对同一
  tid 幂等。每个参与者用 `applies` 计数实际写入次数，恒为 0 或 1。
- **成功即终态**：只有所有参与者都 ack 决定，协调者才返回 committed；
  决定已落盘但通知未完成时返回 `pending`，由 sweeper 与参与者 watcher
  双向兜底推进到终态。无法确定时一律 `pending`，绝不假成功。
- **键锁**：参与者在 prepare 时锁定涉及的键，冲突事务投反对票，协调者
  因而决定整体中止；中止事务的任何值都不会进 kv，持锁事务的锁不会被
  竞争者抢走或误释放。

## 崩溃 / 挂起注入开关

通过环境变量配置（可逗号分隔多个点；`:N` 表示第 N 次命中才触发）：

```bash
CRASH=coord_decision_after python3 coordinator.py ...
BLOCK=p_vote_resp_block:1 BLOCK_TID=T3 python3 participant.py ...
```

`CRASH` 触发时进程 `os._exit(9)` 立即死亡（不刷用户态缓冲、不跑
finally/atexit），只保留已 fsync 的内容。`BLOCK` 触发时该请求线程
永久挂起（进程仍活着、盘上状态不变），用来制造“投票后半死”窗口；
`BLOCK_TID` 把挂起限定到某个事务，避免恢复线程对其它 tid 的重放被
误伤。

### 协调者崩溃点

| 点 | 时机 |
|----|------|
| `coord_insert_before/after` | 受理记录落盘前后 |
| `coord_prepare_send_before/after` | 向参与者发 prepare 前后 |
| `coord_decision_before/after` | **提交/中止决定 fsync 落盘前后** |
| `coord_notify_before/after` | 向每个参与者发 commit/abort 前后 |

### 参与者崩溃点

| 点 | 时机 |
|----|------|
| `p_vote_before` | 投赞成票、写投票+锁之前 |
| `p_vote_after` | 投票+锁已落盘、应答之后 |
| `p_commit_before/after` | 本地写 kv 并提交的事务前后 |
| `p_vote_resp_block`（仅 BLOCK）| 投票已落盘、HTTP 应答之前挂起 |
| `p_commit_resp_block`（仅 BLOCK）| 本地提交后、应答之前挂起 |

## 重启恢复

- 协调者重启：扫描 `txn_log`，`STARTED` 的重入阶段一（已投的票按 tid
  原样重取，幂等），`COMMITTED/ABORTED` 的只重放通知；另有 0.5s
  sweeper 兜底推进所有未完成事务。
- 参与者重启：扫描本地 `txn_log`，对每个 `PREPARED` 事务向协调者查
  `/decision` 后执行；协调者不可达就继续等待，锁不释放。

## 测试

```bash
python3 -m unittest test_2pc -v
```

覆盖：正常提交、相同 tid 串行/并发重试、键锁冲突中止、协调者在决定
落盘后/通知中途崩溃、参与者在投票前后/本地提交后崩溃、协调者宕机窗口
的待决语义，以及两组多线程交错竞争事务（一组无崩溃、一组中途强杀
协调者和参与者）。每次结算后统一核对：

1. 协调者无 `STARTED`、通知全部确认；
2. 参与者的 `locks` 与其 `PREPARED` 记录严格一致（不丢失、不多余）；
3. 已提交事务在每个相关参与者 `COMMITTED` 且 `applies==1`；
4. 已中止事务 `ABORTED` 且 `applies==0`，其值在 kv 中不可见；
5. kv 中每个最终值都归属某个已提交事务（无部分提交/脏写）。
