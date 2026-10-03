"""端到端测试: 真实子进程、真实独立 SQLite 文件、真实崩溃与重启。

直接运行: ``python tests/test_2pc.py``（无需 pytest）
或:        ``python -m pytest tests/test_2pc.py -v``

覆盖:
  1. 基本提交，各参与者独立 SQLite 最终一致
  2. 键锁冲突 -> 全组中止，无人写入
  3. 相同事务 ID 的客户端重试幂等（提交只应用一次）
  4. 参与者“投票落盘后”崩溃：锁不丢、待决不猜测、重启后提交
  5. 参与者“提交落盘后、ack 前”崩溃：协调者待决而非假成功，重启恢复
  6. 协调者“决定落盘后、通知前”崩溃：参与者持锁待决，重启后全部提交
  7. 协调者在 prepare 期间崩溃（残留 preparing）：推定中止，锁全部释放
  8. 竞争事务交错（线程 + 崩溃重启），无部分提交 / 无丢失锁 / 无重复应用
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from twopc import rpc  # noqa: E402

CRASH_POINTS_PARTICIPANT = [
    "participant:vote:after",
    "participant:commit:before",
    "participant:commit:after",
    "participant:abort:before",
    "participant:abort:after",
]
CRASH_POINTS_COORDINATOR = [
    "coordinator:prepare:after",
    "coordinator:commit:decision:before",
    "coordinator:commit:decision:after",
    "coordinator:commit:notify:before",
    "coordinator:commit:notify:after",
    "coordinator:abort:decision:before",
    "coordinator:abort:decision:after",
    "coordinator:abort:notify:before",
    "coordinator:abort:notify:after",
]

CRASH_EXIT_CODE = 99


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class NodeProc:
    """一个角色（参与者/协调者）的进程句柄，可带崩溃开关重启。"""

    def __init__(self, tmpdir: Path, role: str, name: str, port: int,
                 db_name: str, participants=None, log_sink=None) -> None:
        self.tmpdir = tmpdir
        self.role = role
        self.name = name
        self.port = port
        self.db = tmpdir / db_name
        self.participants = participants
        self.log_sink = log_sink
        self.crash_file = tmpdir / f"{name}.crash.json"
        self.proc = None

    def start(self, crash_triggers=None) -> None:
        cmd = [sys.executable, "-m", "twopc", self.role,
               "--host", "127.0.0.1", "--port", str(self.port),
               "--db", str(self.db)]
        if self.role == "participant":
            cmd += ["--name", self.name]
        else:
            spec = ",".join(
                f"{p.name}=127.0.0.1:{p.port}" for p in self.participants
            )
            cmd += ["--participants", spec, "--sweep-interval", "0.1"]
        if crash_triggers is not None:
            self.crash_file.write_text(
                json.dumps({"triggers": crash_triggers}), encoding="utf-8"
            )
            cmd += ["--crash", str(self.crash_file)]
        logf = open(self.tmpdir / f"{self.name}.out", "ab")
        self.logf = logf
        self.proc = subprocess.Popen(
            cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=logf,
            text=True, bufsize=1,
        )
        line = self.proc.stdout.readline()
        if not line.startswith("READY"):
            raise RuntimeError(f"{self.name} failed to start: {line!r}")

    def restart(self, crash_triggers=None) -> None:
        """进程已崩溃（退出码 99）后重启；默认不带新的崩溃开关。"""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)
        if getattr(self, "logf", None) is not None:
            self.logf.close()
            self.logf = None
        self.start(crash_triggers)

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if getattr(self, "logf", None) is not None:
            self.logf.close()
            self.logf = None

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def crashed(self) -> bool:
        return self.proc is not None and self.proc.poll() == CRASH_EXIT_CODE

    def call(self, method, params=None, timeout=5.0):
        return rpc.call(("127.0.0.1", self.port), method, params, timeout)

    def wait_until_dead(self, timeout=10.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.is_alive():
                return
            time.sleep(0.02)
        raise AssertionError(f"{self.name} did not die")

    def wait_until_up(self, timeout=10.0) -> None:
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                self.call("status", {"tid": "__probe__"}, timeout=1.0)
                return
            except rpc.RPCError as exc:
                last = exc
                time.sleep(0.05)
        raise AssertionError(f"{self.name} did not come back: {last}")


class Cluster:
    def __init__(self, n_participants=3) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="twopc-test-"))
        self.participants = []
        for i in range(n_participants):
            name = f"p{i + 1}"
            self.participants.append(
                NodeProc(self.tmp, "participant", name, free_port(),
                         f"{name}.db")
            )
        self.coord = NodeProc(self.tmp, "coordinator", "coord", free_port(),
                              "coord.db", participants=self.participants)

    def start_all(self, coord_crash=None):
        for p in self.participants:
            p.start()
        self.coord.start(coord_crash)

    def stop_all(self):
        self.coord.stop()
        for p in self.participants:
            p.stop()

    def dumps(self):
        return {p.name: p.call("dump") for p in self.participants}

    def submit(self, tid, ops, timeout=5.0):
        return self.coord.call("submit", {"tid": tid, "ops": ops}, timeout)

    def wait_final(self, tid, timeout=10.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            st = self.coord.call("status", {"tid": tid})["state"]
            if st in ("committed", "aborted"):
                return st
            time.sleep(0.05)
        raise AssertionError(f"txn {tid} never reached a final state")


def kv(key, value):
    return {"key": key, "value": value}


class TwoPCTests(unittest.TestCase):
    cluster: Cluster = None

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(n_participants=3)
        cls.cluster.start_all()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop_all()

    # --------------------------------------------------------- 1. 基本提交

    def test_01_basic_commit(self):
        c = self.cluster
        tid = "t-commit"
        res = c.submit(tid, [kv("a", "1"), kv("b", "2")])
        self.assertEqual(res["state"], "committed", res)
        for p in c.participants:
            d = p.call("dump")
            self.assertEqual(d["kv"].get("a"), "1", d)
            self.assertEqual(d["kv"].get("b"), "2", d)
            self.assertEqual(d["locks"], {}, f"locks leaked: {d['locks']}")
            txn = next(t for t in d["txns"] if t["tid"] == tid)
            self.assertEqual(txn["state"], "committed")
            self.assertEqual(txn["commit_count"], 1)

    # --------------------------------------------------- 2. 冲突 -> 全中止

    def test_02_lock_conflict_aborts_everywhere(self):
        c = self.cluster
        # 先制造一个持锁待决的事务：在 p1 投票后让它崩溃，锁留在磁盘上
        p1 = c.participants[0]
        blocker = "t-block"
        p1.restart([{"point": "participant:vote:after", "tid": blocker}])
        try:
            res = c.submit(blocker, [kv("hot", "x")])
            # p1 崩溃导致提交决定只能中止（参与者不可达）
            self.assertEqual(res["state"], "aborted", res)
            p1.wait_until_dead()
            # p1 磁盘上是 prepared + 持锁，参与者不自行决定
            self.assertTrue(p1.crashed())
            # 重启 p1：没有任何提交决定落盘，sweeper/重试将完成 abort
            p1.restart()
            p1.wait_until_up()
            self.assertEqual(c.wait_final(blocker), "aborted")

            # 现在协调者决定为 abort；竞争者用相同的键，应在其余参与者处
            # 拿不到一致 yes（blocker 的 abort 已传播后键才释放）。
            # 先等 blocker 在所有参与者处 aborted、锁清空
            deadline = time.time() + 10
            while time.time() < deadline:
                if all(
                    t["state"] == "aborted"
                    for p in c.participants
                    for t in [next(
                        (x for x in p.call("dump")["txns"]
                         if x["tid"] == blocker), {"state": "?"})]
                ):
                    break
                time.sleep(0.05)

            # blocker 已彻底中止、锁释放：新事务可以正常提交
            nxt = "t-after-abort"
            res = c.submit(nxt, [kv("hot", "y")])
            self.assertEqual(res["state"], "committed", res)
            for p in c.participants:
                d = p.call("dump")
                self.assertEqual(d["kv"].get("hot"), "y")
                self.assertEqual(d["locks"], {})
        finally:
            # 确保后续测试的 p1 健康在线
            if not p1.is_alive():
                p1.restart()
            p1.wait_until_up()

    # --------------------------------------------- 3. 重试幂等 / 只提交一次

    def test_03_duplicate_tid_idempotent(self):
        c = self.cluster
        tid = "t-idem"
        ops = [kv("idem", "100")]
        first = c.submit(tid, ops)
        self.assertEqual(first["state"], "committed", first)
        # 相同事务 ID 客户端重试：返回终态，不重复应用
        second = c.submit(tid, ops)
        self.assertEqual(second["state"], "committed")
        self.assertTrue(second.get("duplicate"))
        for p in c.participants:
            d = p.call("dump")
            txn = next(t for t in d["txns"] if t["tid"] == tid)
            self.assertEqual(txn["commit_count"], 1, f"double apply on {p.name}")
            self.assertEqual(d["kv"]["idem"], "100")

    # ----------------------------- 4. 参与者投票落盘后崩溃：待决不猜测

    def test_04_participant_crash_after_vote_presumed_abort(self):
        c = self.cluster
        p2 = c.participants[1]
        tid = "t-vote-crash"
        # 仅让 p2 在“投票之后”崩溃：协调者收不齐 prepare 应答
        p2.restart([{"point": "participant:vote:after", "tid": tid}])
        try:
            res = c.submit(tid, [kv("vc", "7")])
            # 标准 2PC：prepare 阶段有参与者不可达 -> 决定中止（决定落盘）
            self.assertEqual(res["state"], "aborted", res)
            p2.wait_until_dead()
            self.assertTrue(p2.crashed())

            # p2 磁盘上保留 prepared 记录 + 键锁（崩溃前已 fsync）
            import sqlite3
            conn = sqlite3.connect(p2.db)
            try:
                state = conn.execute(
                    "SELECT state FROM txn_log WHERE tid=?", (tid,)
                ).fetchone()[0]
                locked = conn.execute(
                    "SELECT key FROM locks WHERE tid=?", (tid,)
                ).fetchall()
                val = conn.execute(
                    "SELECT value FROM kv WHERE key='vc'"
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(state, "prepared")
            self.assertEqual(locked, [("vc",)])
            self.assertIsNone(val)  # 待决期间绝不写入数据

            # p2 重启：它仍然 prepared，必须等协调者重发决定，
            # 不自行猜测。协调者 sweeper 把持久的 abort 决定补通知过来。
            p2.restart()
            p2.wait_until_up()
            st = p2.call("status", {"tid": tid})
            self.assertEqual(st["state"], "prepared")  # 刚起来仍是待决
            self.assertEqual(c.wait_final(tid), "aborted")
            deadline = time.time() + 10
            while time.time() < deadline:
                d = p2.call("dump")
                txn = next(t for t in d["txns"] if t["tid"] == tid)
                if txn["state"] == "aborted" and not d["locks"]:
                    break
                time.sleep(0.05)
            d = p2.call("dump")
            txn = next(t for t in d["txns"] if t["tid"] == tid)
            self.assertEqual(txn["state"], "aborted")
            self.assertNotIn("vc", d["kv"])
            self.assertEqual(d["locks"], {})
            # 全组状态一致
            for p in c.participants:
                dd = p.call("dump")
                t = next((x for x in dd["txns"] if x["tid"] == tid), None)
                self.assertIsNotNone(t)
                self.assertEqual(t["state"], "aborted")
                self.assertEqual(dd["locks"], {})
        finally:
            if not p2.is_alive():
                p2.restart()
            p2.wait_until_up()

    # -------------------------- 5. 参与者提交落盘后 ack 前崩溃：不假成功

    def test_05_participant_crash_after_commit_durable(self):
        c = self.cluster
        p3 = c.participants[2]
        tid = "t-commit-ack-crash"
        p3.restart([{"point": "participant:commit:after", "tid": tid}])
        try:
            res = c.submit(tid, [kv("ac", "9")])
            # 决定与所有参与者提交都已落盘，只是 p3 的 ack 丢了
            self.assertEqual(res["state"], "pending", res)
            p3.wait_until_dead()
            self.assertTrue(p3.crashed())

            # 重启前直接检查 p3 数据库文件：提交必须已经持久
            import sqlite3
            conn = sqlite3.connect(p3.db)
            try:
                state = conn.execute(
                    "SELECT state FROM txn_log WHERE tid=?", (tid,)
                ).fetchone()[0]
                value = conn.execute(
                    "SELECT value FROM kv WHERE key='ac'"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(state, "committed")
            self.assertEqual(value, "9")

            p3.restart()
            p3.wait_until_up()
            self.assertEqual(c.wait_final(tid), "committed")
            # 重试 commit 是幂等的：提交只应用一次
            d = p3.call("dump")
            txn = next(t for t in d["txns"] if t["tid"] == tid)
            self.assertEqual(txn["commit_count"], 1)
            self.assertEqual(d["kv"]["ac"], "9")
        finally:
            if not p3.is_alive():
                p3.restart()
            p3.wait_until_up()

    # --------------------------- 6. 协调者决定落盘后通知前崩溃 -> 全提交

    def test_06_coord_crash_between_decision_and_notify(self):
        c = self.cluster
        tid = "t-coord-decision-crash"
        c.coord.restart([
            {"point": "coordinator:commit:decision:after", "tid": tid}
        ])
        c.coord.wait_until_up()
        try:
            try:
                res = c.submit(tid, [kv("dc", "5")])
                self.assertEqual(res["state"], "pending", res)
            except rpc.RPCError:
                pass  # 连接随进程崩溃被重置也算预期
            c.coord.wait_until_dead()
            self.assertTrue(c.coord.crashed())

            # 协调者宕机期间：参与者持锁待决，绝不自行提交/中止
            time.sleep(1.0)
            for p in c.participants:
                st = p.call("status", {"tid": tid})
                self.assertEqual(st["state"], "prepared", (p.name, st))
                self.assertIn("dc", st["locked_keys"])
                got = p.call("get", {"key": "dc"})
                self.assertIsNone(got["value"])
                self.assertEqual(got["locked_by"], tid)

            # 协调者重启：按日志恢复（决定已落盘 -> committing），补通知
            c.coord.restart()
            c.coord.wait_until_up()
            self.assertEqual(c.wait_final(tid), "committed")
            for p in c.participants:
                d = p.call("dump")
                self.assertEqual(d["kv"]["dc"], "5")
                self.assertEqual(d["locks"], {})
        finally:
            if not c.coord.is_alive():
                c.coord.restart()
            c.coord.wait_until_up()

    # ------------------------------- 7. 协调者 prepare 中崩溃 -> 推定中止

    def test_07_coord_crash_in_preparing_presumed_abort(self):
        c = self.cluster
        tid = "t-coord-preparing-crash"
        c.coord.restart([
            {"point": "coordinator:prepare:after", "tid": tid}
        ])
        c.coord.wait_until_up()
        try:
            try:
                c.submit(tid, [kv("pc", "3")])
            except rpc.RPCError:
                pass
            c.coord.wait_until_dead()
            self.assertTrue(c.coord.crashed())

            # 此时部分参与者可能已 prepare 并持锁。等待片刻：
            # 它们必须保持待决，不能自行猜测。
            time.sleep(0.8)
            for p in c.participants:
                st = p.call("status", {"tid": tid})
                self.assertIn(st["state"], ("prepared", "unknown"),
                              (p.name, st))
                if st["state"] == "prepared":
                    self.assertEqual(st["locked_keys"], ["pc"])

            c.coord.restart()
            c.coord.wait_until_up()
            # 重启恢复把残留 preparing 推定为 aborting 并通知所有人
            self.assertEqual(c.wait_final(tid), "aborted")
            for p in c.participants:
                d = p.call("dump")
                self.assertNotIn("pc", d["kv"])
                self.assertEqual(d["locks"], {})
                txn = next((t for t in d["txns"] if t["tid"] == tid), None)
                self.assertIsNotNone(txn)
                self.assertEqual(txn["state"], "aborted")
        finally:
            if not c.coord.is_alive():
                c.coord.restart()
            c.coord.wait_until_up()

    # ------------------- 8b. 参与者收到 commit 通知“之前”崩溃

    def test_09_participant_crash_before_commit_notify(self):
        c = self.cluster
        p1 = c.participants[0]
        tid = "t-commit-before-crash"
        p1.restart([{"point": "participant:commit:before", "tid": tid}])
        try:
            res = c.submit(tid, [kv("bc", "4")])
            # 协调者决定已落盘(committing)，但 p1 在应用提交前死亡
            self.assertEqual(res["state"], "pending", res)
            p1.wait_until_dead()
            self.assertTrue(p1.crashed())

            # p1 磁盘状态仍是 prepared + 持锁，绝不自行提交
            import sqlite3
            conn = sqlite3.connect(p1.db)
            try:
                state = conn.execute(
                    "SELECT state FROM txn_log WHERE tid=?", (tid,)
                ).fetchone()[0]
                locked = conn.execute(
                    "SELECT key FROM locks WHERE tid=?", (tid,)
                ).fetchall()
                val = conn.execute(
                    "SELECT value FROM kv WHERE key='bc'"
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(state, "prepared")
            self.assertEqual(locked, [("bc",)])
            self.assertIsNone(val)

            # 其它参与者已经提交（决定是全局提交）；p1 重启后被补提交
            for p in c.participants[1:]:
                d = p.call("dump")
                self.assertEqual(d["kv"]["bc"], "4")
            p1.restart()
            p1.wait_until_up()
            self.assertEqual(c.wait_final(tid), "committed")
            d = p1.call("dump")
            self.assertEqual(d["kv"]["bc"], "4")
            self.assertEqual(d["locks"], {})
            self.assertEqual(
                next(t for t in d["txns"] if t["tid"] == tid)["state"],
                "committed",
            )
        finally:
            if not p1.is_alive():
                p1.restart()
            p1.wait_until_up()

    # ------------- 8c. 协调者“提交决定落盘前”崩溃：推定中止

    def test_10_coord_crash_before_decision_persisted(self):
        c = self.cluster
        tid = "t-decision-before-crash"
        c.coord.restart([
            {"point": "coordinator:commit:decision:before", "tid": tid}
        ])
        c.coord.wait_until_up()
        try:
            try:
                res = c.submit(tid, [kv("db", "6")])
                self.assertEqual(res["state"], "pending", res)
            except rpc.RPCError:
                pass
            c.coord.wait_until_dead()
            self.assertTrue(c.coord.crashed())

            # 决定还没落盘：参与者只有 prepared，协调者宕机期间必须等待
            time.sleep(0.8)
            for p in c.participants:
                st = p.call("status", {"tid": tid})
                self.assertEqual(st["state"], "prepared", (p.name, st))

            c.coord.restart()
            c.coord.wait_until_up()
            # 没有持久 commit 决定 -> 推定中止，全部回到 aborted
            self.assertEqual(c.wait_final(tid), "aborted")
            for p in c.participants:
                d = p.call("dump")
                self.assertNotIn("db", d["kv"])
                self.assertEqual(d["locks"], {})
                self.assertEqual(
                    next(t for t in d["txns"] if t["tid"] == tid)["state"],
                    "aborted",
                )
        finally:
            if not c.coord.is_alive():
                c.coord.restart()
            c.coord.wait_until_up()

    # ------------------------ 8. 竞争事务交错 + 随机崩溃：全局不变量

    def test_08_interleaved_race_transactions_with_crashes(self):
        c = self.cluster
        # 两组事务：A 组写 key race@1..2，B 组写 race@2..3（键 2 交叠）
        txns = {}
        for i in range(4):
            txns[f"race-A{i}"] = [
                kv(f"race@{(i % 2) + 1}", f"A{i}"),
                kv("race@shared", f"shared-A{i}"),
            ]
        for i in range(4):
            txns[f"race-B{i}"] = [
                kv(f"race@{(i % 2) + 2}", f"B{i}"),
                kv("race@shared", f"shared-B{i}"),
            ]

        outcomes = {}
        outcomes_lock = threading.Lock()
        barrier = threading.Barrier(len(txns))

        def run_one(tid, ops):
            barrier.wait()
            for attempt in range(60):
                try:
                    res = c.submit(tid, ops)
                    state = res["state"]
                except rpc.RPCError:
                    state = "pending"  # 协调者正在崩溃重启
                if state in ("committed", "aborted"):
                    with outcomes_lock:
                        outcomes[tid] = state
                    return
                time.sleep(0.2)
            with outcomes_lock:
                outcomes[tid] = "pending"

        threads = [
            threading.Thread(target=run_one, args=(tid, ops))
            for tid, ops in txns.items()
        ]

        # 同时安排一次协调者在通知窗口的崩溃重启
        def crash_coord_once():
            time.sleep(0.4)
            try:
                if c.coord.is_alive():
                    c.coord.proc.kill()
                    c.coord.wait_until_dead()
                    time.sleep(0.3)
                    c.coord.restart()
                    c.coord.wait_until_up()
            except Exception:
                pass

        crasher = threading.Thread(target=crash_coord_once)
        for t in threads:
            t.start()
        crasher.start()
        crasher.join()
        for t in threads:
            t.join(timeout=60)

        # sweeper 会把所有未落终态的事务继续推完
        deadline = time.time() + 30
        while time.time() < deadline:
            pending = [
                tid for tid in txns
                if c.coord.call("status", {"tid": tid})["state"]
                not in ("committed", "aborted", "unknown")
            ]
            if not pending:
                break
            time.sleep(0.2)
        self.assertEqual(pending, [], f"still pending: {pending}")

        dumps = c.dumps()
        coord_dump = c.coord.call("dump")
        coord_states = {t["tid"]: t["state"] for t in coord_dump["txns"]}

        # ---- 不变量 1: 每个事务全组状态一致（不存在部分提交） ----
        for tid in txns:
            final = coord_states.get(tid)
            self.assertIn(final, ("committed", "aborted"), (tid, final))
            for pname, d in dumps.items():
                txn = next((t for t in d["txns"] if t["tid"] == tid), None)
                self.assertIsNotNone(txn, f"{tid} missing on {pname}")
                self.assertEqual(txn["state"], final,
                                 f"{tid} {pname}: {txn['state']} vs {final}")

        # ---- 不变量 2: 无丢失锁（任何残留锁必须属于一个未终态事务） ----
        for pname, d in dumps.items():
            self.assertEqual(d["locks"], {}, f"{pname} leaked locks: {d}")

        # ---- 不变量 3: 提交只应用一次 ----
        for d in dumps.values():
            for t in d["txns"]:
                if t["tid"].startswith("race-"):
                    self.assertLessEqual(
                        t["commit_count"], 1,
                        f"{t['tid']} applied {t['commit_count']} times"
                    )

        # ---- 不变量 4: 每个键在所有参与者上的最终值一致（无分叉） ----
        all_keys = {o["key"] for ops in txns.values() for o in ops}
        final_values = {}
        for key in sorted(all_keys):
            vals = {d["kv"].get(key) for d in dumps.values()}
            self.assertEqual(len(vals), 1, f"{key} diverged: {vals}")
            final_values[key] = next(iter(vals))

        # ---- 不变量 5: 最终值必须来自某个已提交事务写入的候选值 ----
        for key in sorted(all_keys):
            candidates = {
                o["value"]
                for tid, ops in txns.items()
                if coord_states[tid] == "committed"
                for o in ops if o["key"] == key
            }
            if candidates:
                self.assertIn(final_values[key], candidates,
                              (key, final_values[key], candidates))
            else:
                # 该键的所有写事务都中止了：键必须不存在
                self.assertIsNone(final_values[key], key)


if __name__ == "__main__":
    unittest.main(verbosity=2)
