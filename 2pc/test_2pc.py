"""2PC 端到端测试。

覆盖：
* 基本提交 / 同 tid 重试不重复执行（含并发重试）
* 键锁冲突导致的中止、锁不丢失、中止事务无写入
* 协调者在「决定落盘前后」「通知前后」崩溃的恢复
* 参与者在「投票后」「本地提交后」崩溃的恢复
* 协调者宕机窗口内已准备参与者保持待决、锁保留
* 多线程交错的竞争事务（无崩溃 / 中途强杀协调者与参与者）

最终状态统一核对不变量：
  - 没有 STARTED 事务、没有残留锁
  - 提交事务：所有相关参与者 COMMITTED、applies==1、键值正确
  - 中止事务：所有相关参与者未提交、applies==0、其专属值不出现在 kv
  - kv 中每个值都归属某个已提交事务（无部分提交/脏写）
"""

import json
import os
import random
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import http_json, RpcError  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable

COMMITTED, ABORTED, PREPARED, STARTED = \
    "COMMITTED", "ABORTED", "PREPARED", "STARTED"


def wait_for(cond, timeout=20.0, interval=0.1, desc=""):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = cond()
            if last:
                return last
        except Exception as e:
            last = e
        time.sleep(interval)
    raise AssertionError("timeout waiting for %s (last=%r)" % (desc, last))


def wait_port(url, timeout=10.0):
    def up():
        try:
            http_json(url + "/health", None, timeout=1.0)
            return True
        except Exception:
            return False
    wait_for(up, timeout=timeout, desc="health %s" % url)


class Cluster:
    """管理 1 个协调者 + N 个参与者的子进程，每个节点独立 SQLite 文件。"""

    def __init__(self, base_port, n_participants=3):
        self.tmp = tempfile.mkdtemp(prefix="2pc-test-")
        self.cport = base_port
        self.hpports = [base_port + 10 + i for i in range(n_participants)]
        self.names = ["pa", "pb", "pc"][:n_participants]
        self.coord_url = "http://127.0.0.1:%d" % self.cport
        self.procs = {}          # name -> Popen
        self.crashes = {}        # name -> 当前 CRASH 配置
        self.log_paths = {}

    # ---- 进程管理 ----------------------------------------------------------

    def _popen(self, name, args, crash, block=None, block_tid=None):
        env = dict(os.environ)
        if crash:
            env["CRASH"] = crash
        if block:
            env["BLOCK"] = block
        if block_tid:
            env["BLOCK_TID"] = block_tid
        logp = os.path.join(self.tmp, name + ".log")
        self.log_paths[name] = logp
        logf = open(logp, "ab")
        proc = subprocess.Popen(args, cwd=HERE, env=env, stdout=logf,
                                stderr=subprocess.STDOUT)
        self.procs[name] = proc
        return proc

    def start_coord(self, crash=None, block=None):
        args = [PY, os.path.join(HERE, "coordinator.py"),
                "--db", os.path.join(self.tmp, "coord.db"),
                "--port", str(self.cport),
                "--participants",
                ",".join("%s:%d" % (n, p)
                         for n, p in zip(self.names, self.hpports))]
        self._popen("coord", args, crash, block)
        self.crashes["coord"] = crash
        wait_port(self.coord_url)

    def start_participant(self, name, crash=None, block=None, block_tid=None):
        i = self.names.index(name)
        args = [PY, os.path.join(HERE, "participant.py"),
                "--name", name,
                "--db", os.path.join(self.tmp, name + ".db"),
                "--port", str(self.hpports[i]),
                "--coord", self.coord_url]
        self._popen(name, args, crash, block, block_tid)
        self.crashes[name] = crash
        wait_port("http://127.0.0.1:%d" % self.hpports[i])

    def start_all(self, coord_crash=None, p_crashes=None):
        for n in self.names:
            self.start_participant(n, (p_crashes or {}).get(n))
        self.start_coord(coord_crash)

    def kill(self, name, sig=signal.SIGKILL):
        proc = self.procs.get(name)
        if proc and proc.poll() is None:
            proc.send_signal(sig)
            proc.wait(timeout=10)

    def restart_coord(self, crash=None, block=None):
        self.kill("coord")
        self.start_coord(crash, block)

    def restart_participant(self, name, crash=None, block=None,
                            block_tid=None):
        self.kill(name)
        self.start_participant(name, crash, block, block_tid)

    def shutdown(self):
        for name in list(self.procs):
            self.kill(name, signal.SIGTERM)
        for name, p in list(self.procs.items()):
            try:
                p.wait(timeout=5)
            except Exception:
                self.kill(name)

    def crashed(self, name):
        """进程是否已因崩溃点退出（返回码 9）。"""
        proc = self.procs.get(name)
        return proc is not None and proc.poll() == 9

    # ---- 客户端语义的 RPC --------------------------------------------------

    def transact(self, tid, writes, timeout=10.0):
        try:
            return http_json(self.coord_url + "/transact",
                             {"tid": tid, "writes": writes}, timeout=timeout)
        except (RpcError, OSError) as e:
            return {"state": "rpc-error", "error": repr(e)}

    def p_url(self, name):
        return "http://127.0.0.1:%d" % self.hpports[self.names.index(name)]

    def p_state(self, name):
        return http_json(self.p_url(name) + "/debug/state", None, timeout=3.0)

    def c_state(self):
        return http_json(self.coord_url + "/debug/state", None, timeout=3.0)

    def c_status(self, tid):
        return http_json("%s/status/%s" % (self.coord_url, tid),
                         None, timeout=3.0)

    def wait_crashed(self, name, timeout=10.0):
        wait_for(lambda: self.crashed(name), timeout=timeout,
                 desc="%s crash" % name)

    def wait_settled(self, tids, timeout=40.0):
        """等所有事务在协调者侧到达终态且通知全部确认。"""
        def done():
            for t in tids:
                st = self.c_status(t)["state"]
                if st == STARTED:
                    return False
                s = self.c_status(t)
                if s["notified"] < s["plan_len"]:
                    return False
            return True
        wait_for(done, timeout=timeout, desc="settle")

    def dump_logs(self):
        out = []
        for name, p in self.log_paths.items():
            try:
                with open(p, "rb") as f:
                    out.append("---- %s.log ----\n%s"
                               % (name, f.read().decode("utf-8", "replace")))
            except OSError:
                pass
        return "\n".join(out)


# ---------------------------------------------------------------------------
# 不变量核对
# ---------------------------------------------------------------------------

def assert_txn_invariants(test, cluster, plan_writes, extra_plans=()):
    """plan_writes: {tid: {participant: {key: value}}}。

    extra_plans：同一集群上此前测试遗留、也要一并认可的已提交计划
    （各测试共享一个集群，后面的提交会覆盖前面的键值）。

    竞争事务交错时，一个键可能被多个已提交事务先后覆盖，因此正确的
    全局不变量不是“每个提交者都能看到自己的值”，而是：
      * 最终值必须等于某个【已提交】事务对该键写入的值；
      * 任何中止事务的值都不可见 —— 即不存在部分提交/脏写。
    """
    all_plans = dict(plan_writes)
    for ep in extra_plans:
        all_plans.update(ep)

    cst = cluster.c_state()
    pstates = {n: cluster.p_state(n) for n in cluster.names}

    coord_by_tid = {t["tid"]: t for t in cst["txns"]}
    p_by_tid = {n: {t["tid"]: t for t in s["txns"]}
                for n, s in pstates.items()}

    # 1) 协调者无 STARTED，通知全部确认；所有参与者无残留/丢失的锁。
    started = {t["tid"] for t in cst["txns"] if t["state"] == STARTED}
    for tid in plan_writes:
        ct = coord_by_tid[tid]
        test.assertIn(ct["state"], (COMMITTED, ABORTED), tid)
        test.assertEqual(ct["notified"], len(ct["plan"]), tid)
    # 结算后的事务不得再持锁；只有协调者仍未决(STARTED)的事务允许持锁，
    # 且参与者的锁状态必须与其 PREPARED 记录严格一致（不丢失、不多余）。
    for n, s in pstates.items():
        expected_locks = {}
        for pt in s["txns"]:
            if pt["state"] == PREPARED:
                test.assertIn(pt["tid"], started,
                              "%s: PREPARED %s has no pending coord decision"
                              % (n, pt["tid"]))
                writes_of = next(
                    (pw.get(n, {})
                     for tid, pw in all_plans.items() if tid == pt["tid"]),
                    None)
                test.assertIsNotNone(writes_of, "%s: unknown prepared %s"
                                     % (n, pt["tid"]))
                for k in writes_of:
                    expected_locks[k] = pt["tid"]
        for k, tid in s["locks"].items():
            test.assertIn(tid, started,
                          "%s: lock %s held by settled txn %s" % (n, k, tid))
        test.assertEqual(s["locks"], expected_locks,
                         "%s: lock/state mismatch" % n)

    # 2) 本批事务：参与者状态与协调者决定一致；applies 必须为 0 或 1，
    #    提交者恰好 1（重试绝不重复执行），中止者为 0。
    dump = [json.dumps(cst, sort_keys=True)] + \
           ["%s:%s" % (n, json.dumps(s, sort_keys=True))
            for n, s in sorted(pstates.items())]
    for tid, pw in plan_writes.items():
        cdecision = coord_by_tid[tid]["state"]
        for name in pw:
            pt = p_by_tid[name].get(tid)
            test.assertIsNotNone(pt, "missing txn_log %s@%s\n%s"
                                 % (tid, name, "\n".join(dump)))
            if cdecision == COMMITTED:
                test.assertEqual(pt["state"], COMMITTED,
                                 "%s@%s not committed\n%s"
                                 % (tid, name, "\n".join(dump)))
                test.assertEqual(pt["applies"], 1,
                                 "%s@%s applied %d times\n%s"
                                 % (tid, name, pt["applies"], "\n".join(dump)))
            else:
                test.assertEqual(pt["state"], ABORTED,
                                 "%s@%s should be aborted\n%s"
                                 % (tid, name, "\n".join(dump)))
                test.assertEqual(pt["applies"], 0,
                                 "%s@%s applied despite abort\n%s"
                                 % (tid, name, "\n".join(dump)))

    # 3) 全局所有权检查：
    #    committed_values[(participant,key)] = 已提交事务写入的值集合
    #    aborted_values[(participant,key)] = 中止事务写入的值集合
    committed_values, aborted_values = {}, {}
    for tid, pw in all_plans.items():
        decision = coord_by_tid[tid]["state"]
        bucket = committed_values if decision == COMMITTED else aborted_values
        for name, kv in pw.items():
            for k, v in kv.items():
                bucket.setdefault((name, k), set()).add(v)

    for name, s in pstates.items():
        for k, v in s["kv"].items():
            allowed = committed_values.get((name, k), set())
            test.assertIn(v, allowed,
                          "value %r at %s.%s owned by no COMMITTED txn "
                          "(partial commit/dirty write)" % (v, name, k))
            test.assertNotIn(v, aborted_values.get((name, k), set()),
                             "aborted txn value visible at %s.%s" % (name, k))


# ---------------------------------------------------------------------------
# 测试
# ---------------------------------------------------------------------------

class _2PCBase(unittest.TestCase):
    BASE = 18000
    NPARTS = 3

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(cls.BASE + cls.OFFSET * 100, cls.NPARTS)
        cls.cluster.start_all()
        wait_for(lambda: cls.cluster.c_state() is not None, desc="coord")
        cls.history = {}  # 本类集群上所有已结算事务的计划（累计所有权）

    @classmethod
    def tearDownClass(cls):
        cls.cluster.shutdown()

    def setUp(self):
        # 每个测试方法用独立前缀的 tid，互不干扰。
        self._seq = 0

    def tid(self, hint="t"):
        self._seq += 1
        return "%s-%d-%d-%d" % (
            hint, self.OFFSET, self._seq, int(time.time() * 1000) % 100000)

    def check(self, plan):
        """核对不变量，并把本批计划并入类级历史。"""
        assert_txn_invariants(self, self.cluster, plan,
                              extra_plans=(type(self).history,))
        type(self).history.update(plan)


class TestHappyPath(_2PCBase):
    OFFSET = 1

    def test_01_commit_to_all_participants(self):
        t = self.tid("commit")
        writes = {"pa": {"a1": "v1", "a2": "v2"},
                  "pb": {"b1": "v3"},
                  "pc": {"c1": "v4"}}
        r = self.cluster.transact(t, writes)
        self.assertEqual(r["state"], "committed", r)
        self.check({t: writes})

    def test_02_retry_same_tid_does_not_reexecute(self):
        t = self.tid("retry")
        writes = {"pa": {"dup": "orig"}, "pb": {"dup2": "orig2"}}
        self.assertEqual(self.cluster.transact(t, writes)["state"], "committed")

        # 相同 tid、不同内容：必须是幂等恢复路径，原值不变、只执行一次。
        again = {"pa": {"dup": "HACKED", "newkey": "x"}, "pc": {"z": "z"}}
        r = self.cluster.transact(t, again)
        self.assertEqual(r["state"], "committed", r)
        self.check({t: writes})
        s = self.cluster.p_state("pa")
        self.assertEqual(s["kv"]["dup"], "orig")
        self.assertNotIn("newkey", s["kv"])
        self.assertEqual(
            next(x for x in s["txns"] if x["tid"] == t)["applies"], 1)

    def test_03_concurrent_duplicate_submission(self):
        t = self.tid("concur")
        writes = {"pa": {"cc": "1"}, "pb": {"cc": "2"}, "pc": {"cc": "3"}}
        results = []

        def go():
            results.append(self.cluster.transact(t, writes))

        threads = [threading.Thread(target=go) for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertTrue(all(r["state"] == "committed" for r in results),
                        results)
        self.check({t: writes})


class TestLockConflict(_2PCBase):
    OFFSET = 2

    def test_04_conflicting_key_aborts_and_locks_survive(self):
        # A：让 pa 在“投票已落盘、应答之前”挂住（仅限 ta 这一个 tid，
        # sweeper 对 ta 的重放也会挂住，但不影响别的事务）。于是 ta 在
        # 协调者看来阶段一悬置（STARTED、待决），而 pa 本地已持久
        # PREPARED 并持锁。
        ta = self.tid("hold")
        self.cluster.restart_participant(
            "pa", block="p_vote_resp_block:1", block_tid=ta)
        wa = {"pa": {"hot": "A"}, "pb": {"hotb": "A"}}
        r = self.cluster.transact(ta, wa)
        # 协调者对 pa 的 prepare 没有应答（也没有崩溃）：要么同步返回
        # pending，要么客户端在服务端仍挂着时连接超时（rpc-error）。
        # 两者都表示“未确定”，绝不允许假成功。
        self.assertIn(r["state"], ("pending", "rpc-error"), r)

        # 参与者必须保持 PREPARED 且锁不丢。pb 已投赞成票，同样持锁等待。
        wait_for(lambda: self.cluster.p_state("pa")["locks"].get("hot") == ta,
                 desc="pa lock held")
        time.sleep(1.0)  # 观察窗口：参与者不得自行猜测结论
        pa = self.cluster.p_state("pa")
        self.assertEqual(pa["locks"], {"hot": ta})
        self.assertEqual(
            next(x for x in pa["txns"] if x["tid"] == ta)["state"], PREPARED)
        pb = self.cluster.p_state("pb")
        self.assertEqual(pb["locks"], {"hotb": ta})
        self.assertEqual(
            next(x for x in pb["txns"] if x["tid"] == ta)["state"], PREPARED)

        # B：与 A 在 pa.hot 冲突 -> 协调者必须决定中止；pb 上不冲突的键
        # 随整体中止并放锁。pa 虽挂起，但 B 能立刻在持久锁上发现冲突。
        tb = self.tid("conflict")
        wb = {"pa": {"hot": "B"}, "pb": {"other": "B"}}
        rb = self.cluster.transact(tb, wb)
        self.assertEqual(rb["state"], "aborted", rb)
        self.assertEqual(self.cluster.p_state("pb")["locks"], {"hotb": ta})
        self.assertEqual(self.cluster.p_state("pa")["locks"], {"hot": ta})
        pb_kv = self.cluster.p_state("pb")["kv"]
        self.assertNotIn("other", pb_kv)

        # 恢复 pa：它重放 prepare(原票 yes)，协调者恢复阶段一并提交 A。
        self.cluster.restart_participant("pa")
        self.cluster.wait_settled([ta, tb])
        self.check({ta: wa, tb: wb})


class TestCoordinatorCrashes(_2PCBase):
    OFFSET = 3

    def test_05_decision_durable_before_any_notify(self):
        self.cluster.restart_coord(crash="coord_decision_after")
        t = self.tid("da")
        writes = {"pa": {"d1": "x"}, "pb": {"d2": "x"}, "pc": {"d3": "x"}}
        r = self.cluster.transact(t, writes)
        self.assertEqual(r["state"], "rpc-error")
        self.cluster.wait_crashed("coord")

        # 三个参与者都 PREPARED、持锁、未写 kv。
        def all_prepared():
            for n in self.cluster.names:
                s = self.cluster.p_state(n)
                row = next((x for x in s["txns"] if x["tid"] == t), None)
                if not row or row["state"] != PREPARED:
                    return False
            return True
        wait_for(all_prepared, desc="all prepared before restart")
        for n in self.cluster.names:
            s = self.cluster.p_state(n)
            self.assertTrue(s["locks"])
            self.assertFalse(any(k in s["kv"] for k in writes[n]))

        # 重启协调者（无崩溃点）-> 决定已落盘，只重放通知。
        self.cluster.restart_coord()
        self.cluster.wait_settled([t])
        self.check({t: writes})

    def test_06_crash_after_first_notification(self):
        # 通知完 pa 后崩溃：pa 已提交，pb/pc 仍待决并持锁。
        self.cluster.restart_coord(crash="coord_notify_after:1")
        t = self.tid("mid")
        writes = {"pa": {"m1": "v"}, "pb": {"m2": "v"}, "pc": {"m3": "v"}}
        r = self.cluster.transact(t, writes)
        self.assertEqual(r["state"], "rpc-error")
        self.cluster.wait_crashed("coord")

        def pa_committed_others_prepared():
            pa = self.cluster.p_state("pa")
            if next(x for x in pa["txns"] if x["tid"] == t)["state"] \
                    != COMMITTED:
                return False
            for n in ("pb", "pc"):
                s = self.cluster.p_state(n)
                if next(x for x in s["txns"] if x["tid"] == t)["state"] \
                        != PREPARED:
                    return False
            return True
        wait_for(pa_committed_others_prepared, desc="phase2 partial state")
        # 待决者锁仍在；已提交者锁已释放。
        self.assertEqual(self.cluster.p_state("pa")["locks"], {})
        self.assertTrue(self.cluster.p_state("pb")["locks"])
        self.assertTrue(self.cluster.p_state("pc")["locks"])

        self.cluster.restart_coord()
        self.cluster.wait_settled([t])
        self.check({t: writes})


class TestParticipantCrashes(_2PCBase):
    OFFSET = 4

    def test_07_participant_crash_after_vote(self):
        self.cluster.restart_participant("pa", crash="p_vote_after")
        t = self.tid("pvote")
        writes = {"pa": {"pv": "1"}, "pb": {"pv": "2"}}
        r = self.cluster.transact(t, writes)
        # 票已落盘但响应前进程死亡：协调者拿不准 -> pending，绝不假成功。
        self.assertIn(r["state"], ("pending", "rpc-error"), r)
        self.cluster.wait_crashed("pa")

        # pa 干净重启：PREPARED 记录+锁都在盘上，向协调者拉决定恢复。
        self.cluster.restart_participant("pa")
        self.cluster.wait_settled([t])
        self.check({t: writes})

    def test_08_participant_crash_after_local_commit(self):
        self.cluster.restart_participant("pb", crash="p_commit_after")
        t = self.tid("pcommit")
        writes = {"pa": {"pc1": "v"}, "pb": {"pc2": "v"}}
        r = self.cluster.transact(t, writes)
        self.assertIn(r["state"], ("pending", "rpc-error"), r)
        self.cluster.wait_crashed("pb")

        self.cluster.restart_participant("pb")
        self.cluster.wait_settled([t])
        self.check({t: writes})

    def test_09_participant_down_before_vote_is_pending_not_abort(self):
        # 投票记录落盘前崩溃：协调者 prepare 失败 => 待决，而不是擅自中止。
        self.cluster.restart_participant("pc", crash="p_vote_before")
        t = self.tid("pdown")
        writes = {"pa": {"dn": "1"}, "pc": {"dn2": "2"}}
        r = self.cluster.transact(t, writes)
        self.assertEqual(r["state"], "pending", r)
        self.cluster.wait_crashed("pc")

        self.cluster.restart_participant("pc")
        self.cluster.wait_settled([t])
        self.check({t: writes})


class TestInterleavedNoCrash(_2PCBase):
    OFFSET = 5

    def test_10_competing_transactions_interleaved(self):
        random.seed(20261002)
        ntxn = 24
        plan, threads = {}, []

        def submitter(tid, w):
            self.cluster.transact(tid, w, timeout=20.0)

        tids = []
        for i in range(ntxn):
            t = "race-%d-%d" % (self.OFFSET, i)
            tids.append(t)
            owners = random.sample(self.cluster.names, 2)
            plan[t] = {
                n: {"shared-%d" % random.randrange(5):
                    "owner=%s#%d" % (t, i)}
                for n in owners
            }
            th = threading.Thread(target=submitter, args=(t, plan[t]))
            threads.append(th)
        for th in threads:
            th.start()
            time.sleep(random.random() * 0.02)  # 制造交错启动
        for th in threads:
            th.join()

        self.cluster.wait_settled(tids, timeout=45.0)
        self.check(plan)


class TestInterleavedWithCrashes(_2PCBase):
    OFFSET = 6

    def test_11_competing_transactions_with_node_kills(self):
        random.seed(99)
        ntxn = 18
        plan, threads = {}, []
        tids = ["storm-%d-%d" % (self.OFFSET, i) for i in range(ntxn)]

        def submitter(tid, w):
            # 客户端语义：失败/待决就用同一 tid 重试，永不换号、永不重复执行。
            for _ in range(60):
                r = self.cluster.transact(tid, w, timeout=15.0)
                if r["state"] in ("committed", "aborted"):
                    return
                time.sleep(0.4)

        for i, t in enumerate(tids):
            owners = random.sample(self.cluster.names, 2)
            plan[t] = {
                n: {"hot-%d" % random.randrange(4):
                    "owner=%s#%d" % (t, i)}
                for n in owners
            }
            threads.append(threading.Thread(
                target=submitter, args=(t, plan[t])))

        for th in threads:
            th.start()
            time.sleep(random.random() * 0.03)

        # 风暴进行中：强杀协调者与一个参与者，再陆续拉起。
        time.sleep(0.8)
        self.cluster.kill("coord")
        time.sleep(0.8)
        self.cluster.kill("pb")
        time.sleep(0.8)
        self.cluster.start_coord()
        time.sleep(0.8)
        self.cluster.start_participant("pb")

        for th in threads:
            th.join(timeout=60)
            self.assertFalse(th.is_alive(), "submitter stuck")

        self.cluster.wait_settled(tids, timeout=60.0)
        self.check(plan)


if __name__ == "__main__":
    unittest.main(verbosity=2)
