# 两阶段提交（2PC）演示

一个协调者 + 2～3 个使用**独立 SQLite 文件**的参与者，事务内容是向各参与者
写入同一组键值。包含崩溃注入与基于日志的崩溃恢复。

## 目录结构

```
twopc/
  rpc.py          行分隔 JSON 的 TCP RPC（每连接一个请求，天然支持幂等重试）
  participant.py  参与者：kv / txn_log / locks 三张表，投票先落盘再回复
  coordinator.py  协调者：事务日志 + 决定先落盘再通知 + 后台恢复扫描
  __main__.py     进程入口（participant / coordinator 两个子命令）
tests/
  test_2pc.py     真实子进程、真实崩溃(kill)重启、竞争事务交错的端到端测试
```

## 核心语义（与题面逐条对应）

| 要求 | 实现 |
| --- | --- |
| 参与者独立 SQLite 文件 | 每个 `participant --db X.db` 独立文件，WAL + `synchronous=FULL`，commit 返回即 fsync |
| prepare 阶段持久记录投票并锁键 | `txn_log` 写入 `prepared` 与 `locks` 行在**同一个事务**里提交，成功后才回 yes；no 票同样落盘 |
| 协调者先持久决定再通知 | `commit/abort` 决定先写入协调者日志（fsync），之后才向任何参与者发 commit/abort |
| 协调者不可达时参与者保持待决 | prepared 的参与者重启后仍是 prepared 且继续持锁，没有“超时自动提交/中止”，只能等协调者重发决定 |
| 相同事务 ID 重试不重复执行 | 协调者按 tid 串行化（进行中的并发重试直接返回 `pending`）；参与者 commit/abort 幂等，已提交事务 `commit_count` 恒为 1 |
| 成功响应 ⇒ 全部参与者最终可恢复到提交 | 只有所有参与者 ack（本地提交已落盘）才返回 `committed`；ack 不全一律 `pending`，由后台 sweeper 补完 |
| 无法确定时返回待决 | 参与者连接失败/UNKNOWN_TXN 等一律不假装成功，返回 `pending` + `waiting_on` |
| 重启后按日志恢复 | 协调者启动与后台扫描：`committing→补 commit`、`aborting→补 abort`、残留 `preparing→推定中止`；通知进度按 `txn_ack` 断点续传 |
| 不发生部分提交 | 参与者本地“写 kv + 记 committed + 解锁”是一个原子事务；测试逐节点比对状态一致性 |
| 不丢锁 | 锁是磁盘表，崩溃不丢；仅在提交/中止的同一原子事务里释放 |
| 竞争事务顺序一致 | 协调者维护键所有权表，作为冲突事务的全局串行化点；参与者磁盘键锁兜底，避免不同参与者以相反顺序应用两个已提交事务 |

> 说明：经典 2PC 只保证原子性，不保证跨节点冲突可串行化。这里由协调者
> 额外充当冲突串行化点（内存键锁表 + 参与者磁盘键锁），保证竞争事务在
> 所有参与者上以相同顺序生效。协调者崩溃后键锁表清空，由恢复扫描按
> 日志顺序重新接回。

## 崩溃注入开关

通过 `--crash plan.json` 传入，进程命中对应点立即以退出码 99 退出
（`os._exit`，模拟被 kill -9），开关一次性，重启不带开关即正常恢复。

参与者注入点：

- `participant:vote:after`        投票(含锁)落盘后
- `participant:commit:before`     收到 commit、应用提交之前
- `participant:commit:after`      提交落盘后、ack 之前
- `participant:abort:before` / `participant:abort:after`

协调者注入点：

- `coordinator:prepare:after`                 prepare 扇出之后、决定之前
- `coordinator:commit:decision:before/after`  提交决定落盘的前/后
- `coordinator:commit:notify:before/after`    commit 通知的前/后
- `coordinator:abort:decision:before/after`
- `coordinator:abort:notify:before/after`

plan 文件示例：

```json
{"triggers": [{"point": "coordinator:commit:decision:after", "tid": "t1"}]}
```

## 运行

启动 2 个参与者和协调者：

```bash
python -m twopc participant --name p1 --port 9101 --db /tmp/p1.db
python -m twopc participant --name p2 --port 9102 --db /tmp/p2.db
python -m twopc coordinator --port 9100 --db /tmp/c.db \
  --participants p1=127.0.0.1:9101,p2=127.0.0.1:9102
```

提交事务（Python 示例）：

```python
from twopc import rpc
rpc.call(("127.0.0.1", 9100), "submit",
         {"tid": "t1", "ops": [{"key": "a", "value": "1"}]})
# -> {'tid': 't1', 'state': 'committed'}
rpc.call(("127.0.0.1", 9100), "status", {"tid": "t1"})
```

响应状态：`committed` / `aborted`（终态），`pending`（待决，需重试或等待
后台恢复）；重复 tid 的重试返回终态并带 `duplicate: true`。

## 测试

```bash
python tests/test_2pc.py        # 不需要 pytest
# 或
python -m pytest tests/test_2pc.py -v
```

10 个用例：基本提交、锁冲突全组中止、重复 tid 幂等（提交只应用一次）、
参与者投票后崩溃持锁待决、参与者 commit 落盘后 ack 前崩溃（不假成功）、
参与者收到 commit 前崩溃（重启补提交）、协调者决定落盘前/后崩溃、
协调者 prepare 中崩溃推定中止，以及 8 个竞争事务线程交错 + 协调者中途
被 kill 的压力场景，校验：

1. 每个事务在所有参与者上的终态与协调者一致（无部分提交）；
2. 无残留/丢失锁；
3. 每个提交 `commit_count == 1`（无重复执行）；
4. 每个键在所有参与者上的最终值一致，且等于某个已提交事务写入的值。
