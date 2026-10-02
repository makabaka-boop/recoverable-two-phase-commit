"""2PC 协调者。

SQLite 日志（单文件 coord.db）：

    txn_log(tid PRIMARY KEY, state, plan, phase1, notified, created_at)

state:
    STARTED   已受理，正在/准备进行阶段一
    PREPARED  所有参与者都投赞成票（阶段一结果已落盘，但尚非最终决定）
    COMMITTED 提交决定已 fsync 落盘        —— 最终结论
    ABORTED   中止决定已 fsync 落盘        —— 最终结论

语义保证：
* COMMIT/ABORT 决定先落盘（synchronous=FULL 的单个 UPDATE 提交即
  fsync），然后才向任何参与者发出 commit/abort 通知。崩溃在
  “决定落盘之后、通知发出之前”也不改变结论：重启按日志重放通知。
* 同一 tid 的客户端重试绝不重复执行：tid 是幂等键，后续调用只
  读取/推进已存在的事务，返回同样的最终结果。
* 只有“所有 plan 中的参与者都已 ack 决定”才对客户端返回 committed；
  决定已落盘但通知未完成时返回 pending（后台继续推进，最终必达）。
* 决定无法确定（如阶段一还没结束、参与者状态不明）时返回 pending，
  绝不假成功。
"""

import argparse
import contextlib
import json
import sqlite3
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from common import RpcError, http_json, log, maybe_crash

STARTED = "STARTED"
PREPARED = "PREPARED"
COMMITTED = "COMMITTED"
ABORTED = "ABORTED"


class Coordinator:
    def __init__(self, db_path, participants):
        self.db_path = db_path
        # participants: [{"name","url"}]
        self.participants = participants
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        # 每个 tid 一把线程锁，串行化同一事务的重试/恢复推进；
        # 不同事务（包括竞争不同键/相同键的事务）可以真正交错执行。
        self._txn_locks = defaultdict(threading.RLock)
        self._locks_guard = threading.Lock()
        # sweeper 在途去重：同一事务上一轮推进还没结束就不再起新线程。
        self._inflight = set()
        self._inflight_guard = threading.Lock()
        # 关键：sqlite3 的事务是“连接级”的，而这里所有线程共用一个连接。
        # 必须用一把进程内锁把每个“若干条语句 + commit”的落盘单元整个
        # 串行化，否则线程 A 的 commit 会把线程 B 尚未填好 plan 的 INSERT
        # 一起提交（曾导致 plan=[] 的幽灵 COMMITTED）。
        self._dbw = threading.RLock()

    @contextlib.contextmanager
    def _writetxn(self):
        """持有连接写锁直到 commit 完成，保证落盘单元不被其他线程穿插。"""
        with self._dbw:
            delay = 0.02
            for _ in range(20):
                try:
                    yield
                    self.conn.commit()
                    return
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e) and "busy" not in str(e):
                        raise
                    time.sleep(delay)
                    delay = min(delay * 2, 0.5)
            raise RuntimeError("sqlite write lock retries exhausted")

    def _db_execute(self, sql, params=()):
        with self._dbw:
            return self.conn.execute(sql, params)

    def _init_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS txn_log (
                tid        TEXT PRIMARY KEY,
                state      TEXT NOT NULL,
                plan       TEXT NOT NULL DEFAULT '[]',
                phase1     TEXT NOT NULL DEFAULT '{}',
                notified   INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL
            );
            """
        )

    def txn_lock(self, tid):
        # 必须在保护锁内 get-or-create：defaultdict 并发访问会让两个线程
        # 各自创建并拿到不同的 RLock，等于没锁（会出现同一 tid 两条
        # 并发受理、plan 被互相覆盖）。
        with self._locks_guard:
            return self._txn_locks[tid]

    def _row(self, tid):
        cur = self.conn.execute(
            "SELECT tid,state,plan,phase1,notified FROM txn_log WHERE tid=?",
            (tid,),
        )
        return cur.fetchone()

    # ---- 客户端入口 --------------------------------------------------------

    def transact(self, tid, writes_by_participant):
        """执行（或恢复/重试）一个事务。

        writes_by_participant: {participant_name: {key: value}}
        返回 {state: committed|aborted|pending}。
        """
        with self.txn_lock(tid):
            row = self._row(tid)
            if row is None:
                names = set(writes_by_participant)
                known = {p["name"] for p in self.participants}
                unknown = names - known
                if unknown:
                    raise ValueError("unknown participants: %s" % sorted(unknown))
                # plan 同时保存各参与者要写的键值；恢复阶段一需要重放
                # prepare，幂等性依赖 tid 而不是客户端再次提供内容。
                plan = [{"name": p["name"], "url": p["url"],
                         "writes": writes_by_participant.get(p["name"], {})}
                        for p in self.participants
                        if p["name"] in writes_by_participant]
                # 受理记录先落盘，崩溃后重试的同一 tid 能在这里找到自己，
                # 而不会被当成全新事务第二次执行。
                maybe_crash(self_name(), "coord_insert_before")
                with self._writetxn():
                    self.conn.execute(
                        "INSERT INTO txn_log(tid,state,plan,phase1,notified,created_at) "
                        "VALUES(?,?, '[]', '{}', 0, ?)",
                        (tid, STARTED, time.time()),
                    )
                    self.conn.execute(
                        "UPDATE txn_log SET plan=? WHERE tid=?",
                        (json.dumps(plan, sort_keys=True), tid),
                    )
                maybe_crash(self_name(), "coord_insert_after")
                log_role("COORD", "tid=%s accepted, plan=%s"
                         % (tid, [p["name"] for p in plan]))
                row = self._row(tid)
            return self._drive(tid, row)

    # ---- 核心状态机 --------------------------------------------------------

    def _drive(self, tid, row):
        """按日志把事务向前推进。每个分支都从崩溃点安全重入。"""
        state = row["state"]

        if state == STARTED:
            decided = self._phase1(tid, row)
            row = self._row(tid)
            if not decided:
                # 阶段一没跑完就崩溃/出错：还没有最终结论。
                return {"state": "pending", "tid": tid}
            state = row["state"]  # COMMITTED / ABORTED，均已落盘

        if state in (COMMITTED, ABORTED):
            self._phase2(tid, row)
            row = self._row(tid)
            plan = json.loads(row["plan"])
            if row["notified"] >= len(plan):
                log_role("COORD", "tid=%s fully %s" % (tid, state.lower()))
                return {"state": "committed" if state == COMMITTED else "aborted",
                        "tid": tid}
            # 决定不可撤销，但还有参与者没确认 -> 待决，不是失败也不是假成功。
            return {"state": "pending", "tid": tid, "decision": state.lower()}

        return {"state": "pending", "tid": tid}

    # ---- 阶段一：prepare 投票（并发发出，一票悬置则整体待决）-------------

    def _phase1(self, tid, row):
        """返回 True 表示本调用内已产生落盘决定；False 表示尚无最终结论。"""
        plan = json.loads(row["plan"])
        persisted = json.loads(row["phase1"])
        phase1 = dict(persisted)  # 重入时已拿到的票不重复索取
        votes, errors = {}, {}
        vlock = threading.Lock()

        def ask(p):
            name = p["name"]
            # 瞬时连接错误（对端线程池短暂排队等）做几次短重试；真正宕机/
            # 挂起则在有限重试后登记失败，阶段一如约悬置为待决。
            # 总等待刻意保持在客户端超时以内，使调用方能拿到 pending
            # 而不是连接被中途切断的 rpc-error。
            attempt_err = None
            for attempt in range(3):
                try:
                    maybe_crash("COORD", "coord_prepare_send_before")
                    resp = http_json(
                        p["url"] + "/prepare",
                        {"tid": tid, "writes": p.get("writes", {})},
                        timeout=2.0)
                    maybe_crash("COORD", "coord_prepare_send_after")
                    vote = resp.get("vote")
                    break
                except (RpcError, OSError) as e:
                    attempt_err = e
                    time.sleep(0.2)
            else:
                with vlock:
                    errors[name] = "%r" % attempt_err
                return
            with vlock:
                votes[name] = vote
            log_role("COORD", "tid=%s vote from %s: %s" % (tid, name, vote))

        threads = [threading.Thread(target=ask, args=(p,))
                   for p in plan if p["name"] not in persisted]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        phase1.update(votes)
        if errors:
            log_role("COORD", "tid=%s prepare failures %s; pending"
                     % (tid, errors))
            self._persist_phase1(tid, phase1, STARTED)
            return False
        # 理论上 join 后要么有票要么登记错误；防御性检查。
        if any(p["name"] not in phase1 for p in plan):
            self._persist_phase1(tid, phase1, STARTED)
            return False

        all_yes = all(v == "yes" for v in phase1.values())
        self._persist_phase1(tid, phase1, STARTED)

        decision = COMMITTED if all_yes else ABORTED
        # ★ 决定先 fsync 落盘，之后才允许通知任何人。
        maybe_crash("COORD", "coord_decision_before")
        with self._writetxn():
            self.conn.execute(
                "UPDATE txn_log SET state=? WHERE tid=?", (decision, tid)
            )
        maybe_crash("COORD", "coord_decision_after")
        log_role("COORD", "tid=%s decision=%s DURABLE" % (tid, decision))
        return True

    def _persist_phase1(self, tid, phase1, state):
        with self._writetxn():
            self.conn.execute(
                "UPDATE txn_log SET phase1=?, state=? WHERE tid=?",
                (json.dumps(phase1, sort_keys=True), state, tid),
            )

    # ---- 阶段二：通知决定（幂等，可重放）----------------------------------

    def _phase2(self, tid, row):
        plan = json.loads(row["plan"])
        decision = row["state"]
        path = "/commit" if decision == COMMITTED else "/abort"
        notified = row["notified"]

        # notified 是“前缀已确认数”，崩溃重入时从头重放也安全：
        # commit/abort 在参与者端都是幂等的。
        for i, p in enumerate(plan):
            if i < notified:
                continue
            try:
                maybe_crash("COORD", "coord_notify_before")
                resp = http_json(p["url"] + path, {"tid": tid}, timeout=3.0)
                maybe_crash("COORD", "coord_notify_after")
            except (RpcError, OSError) as e:
                # 409 UNKNOWN 也算失败：参与者可能丢了 prepare 记录，
                # 稍后重试（参与者的 watcher 也可能反过来拉决定）。
                log_role("COORD", "tid=%s notify %s %s failed: %r"
                         % (tid, p["name"], decision, e))
                return
            if resp.get("state") not in (decision,):
                log_role("COORD", "tid=%s unexpected ack from %s: %s"
                         % (tid, p["name"], resp))
                return
            notified = i + 1
            with self._writetxn():
                self.conn.execute(
                    "UPDATE txn_log SET notified=? WHERE tid=?",
                    (notified, tid),
                )
            log_role("COORD", "tid=%s %s acked by %s (%d/%d)"
                     % (tid, decision.lower(), p["name"], notified, len(plan)))

    # ---- 查询（参与者恢复时拉决定 / 客户端查状态）-------------------------

    def decision(self, tid):
        # 只读：不取 per-tid 写锁，避免被阶段一/二进行中的线程挡住
        # （恢复中的参与者正是靠这个端点拉决定）。
        row = self._row(tid)
        if row is None:
            # 协调者也没记录：不能瞎猜，让参与者继续等。
            return {"tid": tid, "state": "UNKNOWN"}
        if row["state"] in (COMMITTED, ABORTED):
            return {"tid": tid, "state": row["state"]}
        return {"tid": tid, "state": row["state"]}

    def get_status(self, tid):
        row = self._row(tid)
        if row is None:
            return {"tid": tid, "state": "unknown", "notified": 0,
                    "plan_len": 0, "phase1": {}}
        return {
            "tid": tid,
            "state": row["state"],
            "notified": row["notified"],
            "plan_len": len(json.loads(row["plan"])),
            "phase1": json.loads(row["phase1"]),
        }

    # ---- 重启恢复 ----------------------------------------------------------

    def recover(self):
        """按日志续跑所有未完成事务。

        * STARTED：重入阶段一。票是幂等的：已 PREPARED 的参与者对同一
          tid 的重复 prepare 原样返回 yes，投过 no 的原样返回 no。
        * COMMITTED/ABORTED：决定不可改变，只重放通知。
        """
        rows = self.conn.execute(
            "SELECT tid,state,plan,notified FROM txn_log ORDER BY created_at"
        ).fetchall()
        unfinished = [r["tid"] for r in rows
                      if r["state"] == STARTED
                      or r["notified"] < len(json.loads(r["plan"]))]
        if rows:
            log_role("COORD", "recovery: %d logged txns, resuming %s"
                     % (len(rows), unfinished))
        for tid in unfinished:
            threading.Thread(target=self._resume_in_thread,
                             args=(tid,), daemon=True).start()
        threading.Thread(target=self._sweeper, daemon=True).start()

    def _resume_in_thread(self, tid):
        # 原子的“检查并登记”，防止 sweeper 多个 tick 为同一事务并发起线程
        # （并发推进同一 tid 会产生 SQLite 写竞争与状态错乱）。
        with self._inflight_guard:
            if tid in self._inflight:
                return
            self._inflight.add(tid)
        try:
            with self.txn_lock(tid):
                row = self._row(tid)
                if row is None:
                    return
                try:
                    self._drive(tid, row)
                except Exception as e:
                    log_role("COORD", "tid=%s resume error: %r" % (tid, e))
        finally:
            with self._inflight_guard:
                self._inflight.discard(tid)

    def _sweeper(self):
        """兜底：周期推进所有未完成事务（通知期间崩溃、参与者暂时不可达
        等场景），直到每个已决定事务的全部参与者都确认。"""
        while True:
            time.sleep(0.5)
            try:
                rows = self.conn.execute(
                    "SELECT tid,state,plan,notified FROM txn_log"
                ).fetchall()
                for r in rows:
                    unfinished = (
                        r["state"] == STARTED
                        or (r["state"] in (COMMITTED, ABORTED)
                            and r["notified"] < len(json.loads(r["plan"])))
                    )
                    if unfinished:
                        threading.Thread(target=self._resume_in_thread,
                                         args=(r["tid"],), daemon=True).start()
            except Exception as e:
                log_role("COORD", "sweeper error: %r" % e)

    def debug_state(self):
        txns = [dict(r) for r in self.conn.execute(
            "SELECT tid,state,notified,plan FROM txn_log ORDER BY tid")]
        for t in txns:
            t["plan"] = [p["name"] for p in json.loads(t["plan"])]
        return {"txns": txns}


# 模块级角色助手（maybe_crash 只需要一个角色字符串）。
def self_name():
    return "COORD"


def log_role(role, msg):
    log(role, msg)


# ---- HTTP 服务 -------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    coord = None

    def _send(self, code, obj):
        body = json.dumps(obj, sort_keys=True).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        if n == 0:
            return {}
        return json.loads(self.rfile.read(n).decode("utf-8"))

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        c = self.coord
        try:
            if self.path == "/health":
                self._send(200, {"ok": True})
            elif self.path.startswith("/decision/"):
                tid = self.path.rsplit("/", 1)[-1]
                self._send(200, c.decision(tid))
            elif self.path.startswith("/status/"):
                tid = self.path.rsplit("/", 1)[-1]
                self._send(200, c.get_status(tid))
            elif self.path == "/debug/state":
                self._send(200, c.debug_state())
            else:
                self._send(404, {"error": "not found"})
        except Exception as e:
            self._send(500, {"error": "%r" % e})

    def do_POST(self):
        c = self.coord
        try:
            if self.path == "/transact":
                body = self._read_json()
                result = c.transact(body["tid"], body["writes"])
                self._send(200, result)
            else:
                self._send(404, {"error": "not found"})
        except KeyError as e:
            self._send(400, {"error": "missing field %s" % e})
        except ValueError as e:
            self._send(400, {"error": str(e)})
        except Exception as e:
            log("COORD", "handler error: %r" % e)
            self._send(500, {"error": "%r" % e})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--participants", required=True,
                    help="name:port 逗号分隔")
    args = ap.parse_args()

    parts = []
    for item in args.participants.split(","):
        name, port = item.split(":")
        parts.append({"name": name,
                      "url": "http://127.0.0.1:%s" % port})

    c = Coordinator(args.db, parts)
    c.recover()

    _Handler.coord = c
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), _Handler)
    log("COORD", "up on port %d db=%s participants=%s"
        % (args.port, args.db, [p["name"] for p in parts]))
    httpd.serve_forever()


if __name__ == "__main__":
    main()
