"""2PC 参与者。

每个参与者使用独立的 SQLite 文件，包含四类表：

    kv(key TEXT PRIMARY KEY, value TEXT)      实际键值数据
    txn_log(tid PRIMARY KEY, state, writes,   2PC 事务日志（含 prepare 投票）
            applies INTEGER, created_at)
    locks(key TEXT PRIMARY KEY, tid TEXT)     被未决事务持有的键锁
    meta(key PRIMARY KEY, value)              崩溃点命中等内部元数据

状态机：
    PREPARED --commit--> COMMITTED   （写入 kv、记录 applies、释放锁，同一事务内）
    PREPARED --abort---> ABORTED     （仅释放锁）

关键不变量：
* 投票(PREPARED 记录 + 锁)在回复协调者之前就在一个 SQLite 事务里
  fsync 落盘 —— prepare 崩溃重启后协调者重查仍能拿到同样的票。
* PREPARED 期间键锁一直保留；协调者不可达时只能保持待决，参与者
  自己永远不会替协调者做提交/中止结论。
* commit/abort 对非 PREPARED 状态都按幂等处理（重复通知安全）。
* applies 记录每个事务实际写入 kv 的次数，正常恢复后必须为 0 或 1，
  用于测试核对“没有重复执行”。
"""

import argparse
import json
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from common import http_json, log, maybe_block, maybe_crash

PREPARED = "PREPARED"
COMMITTED = "COMMITTED"
ABORTED = "ABORTED"


class Participant:
    def __init__(self, name, db_path, coord_url):
        self.name = name
        self.db_path = db_path
        self.coord_url = coord_url
        # 进程内一把大锁串行化所有写事务，配合 BEGIN IMMEDIATE，
        # 彻底避开 SQLite 写锁竞争（参与者之间本来就是独立文件）。
        self.lock = threading.RLock()
        # 与协调者同理：sqlite3 事务是连接级的，HTTP 线程与恢复 watcher
        # 共用同一连接，需要一把锁把“语句 + commit”的落盘单元整个串行化。
        self.dbw = threading.RLock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    # ---- 存储 --------------------------------------------------------------

    def _init_schema(self):
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS txn_log (
                    tid        TEXT PRIMARY KEY,
                    state      TEXT NOT NULL,
                    writes     TEXT NOT NULL DEFAULT '{}',
                    applies    INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS locks (
                    key TEXT PRIMARY KEY,
                    tid TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    def _get_log(self, tid):
        cur = self.conn.execute(
            "SELECT tid, state, writes, applies FROM txn_log WHERE tid=?",
            (tid,),
        )
        return cur.fetchone()

    # ---- 2PC 阶段一：prepare ----------------------------------------------

    def prepare(self, tid, writes):
        """返回 ('vote-yes', PREPARED) / ('vote-no', ABORTED) /
        ('conflict', state)。

        幂等：同一 tid 重复 prepare 按已持久的记录应答，不重复加锁。
        """
        writes = dict(writes)
        with self.lock:
            row = self._get_log(tid)
            if row is not None:
                # 同事务重试：必须复现原结论，绝不重新评估锁冲突，
                # 否则同一 tid 可能两次得到不同的票。已 PREPARED 就是
                # 持久赞成票 —— 协调者恢复阶段一会原样重发 prepare。
                if row["state"] == PREPARED:
                    return "vote-yes", PREPARED
                return "vote-no", row["state"]

            # 检查键锁冲突：被其他未决(PREPARED)事务持有的键不可写。
            keys = sorted(writes.keys())
            holders = {}
            if keys:
                placeholders = ",".join("?" for _ in keys)
                cur = self.conn.execute(
                    "SELECT key, tid FROM locks WHERE key IN (%s)" % placeholders,
                    keys,
                )
                for r in cur.fetchall():
                    holders[r["key"]] = r["tid"]
            if holders:
                # 投反对票也要留痕，保证对同一 tid 的重复 prepare 永远 no。
                with self.dbw, self.conn:  # BEGIN IMMEDIATE ... COMMIT（已 fsync）
                    self.conn.execute(
                        "INSERT INTO txn_log(tid,state,writes,applies,created_at) "
                        "VALUES(?,?,?,0,?)",
                        (tid, ABORTED, json.dumps(writes, sort_keys=True), time.time()),
                    )
                log(self.name, "tid=%s vote NO, lock conflict: %s"
                    % (tid, holders))
                return "vote-no", ABORTED

            # 投赞成票：投票记录 + 键锁在同一个事务里原子落盘。
            maybe_crash(self.name, "p_vote_before")
            with self.dbw, self.conn:
                self.conn.execute(
                    "INSERT INTO txn_log(tid,state,writes,applies,created_at) "
                    "VALUES(?,?,?,0,?)",
                    (tid, PREPARED, json.dumps(writes, sort_keys=True), time.time()),
                )
                self.conn.executemany(
                    "INSERT INTO locks(key,tid) VALUES(?,?)",
                    [(k, tid) for k in keys],
                )
            # 此刻崩溃也无所谓：重启后 txn_log 里就是 PREPARED 且锁仍在。
            maybe_crash(self.name, "p_vote_after")
            log(self.name, "tid=%s vote YES, locked %s" % (tid, keys))
            return "vote-yes", PREPARED

    # ---- 2PC 阶段二：commit / abort ---------------------------------------

    def commit(self, tid):
        """协调者的提交决定是最终结论。幂等：重复提交只认第一次。"""
        with self.lock:
            row = self._get_log(tid)
            if row is None:
                # 协议上不该出现（决定只能发给投过票的参与者）。
                # 不猜测、不补写：返回 UNKNOWN，让协调者/客户端稍后重试。
                log(self.name, "tid=%s commit for unknown txn -> UNKNOWN" % tid)
                return "UNKNOWN"
            if row["state"] == COMMITTED:
                return COMMITTED  # 重放通知，安全 ack
            if row["state"] == ABORTED:
                # 决定与本地已持久状态冲突 —— 2PC 下不会发生，显式报错。
                raise RuntimeError("tid=%s COMMIT but local state ABORTED" % tid)

            writes = json.loads(row["writes"])
            maybe_crash(self.name, "p_commit_before")
            with self.dbw, self.conn:
                for k, v in writes.items():
                    self.conn.execute(
                        "INSERT INTO kv(key,value) VALUES(?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (k, v),
                    )
                self.conn.execute(
                    "UPDATE txn_log SET state=?, applies=applies+1 WHERE tid=?",
                    (COMMITTED, tid),
                )
                self.conn.execute("DELETE FROM locks WHERE tid=?", (tid,))
            maybe_crash(self.name, "p_commit_after")
            log(self.name, "tid=%s COMMITTED %d keys" % (tid, len(writes)))
            return COMMITTED

    def abort(self, tid):
        with self.lock:
            row = self._get_log(tid)
            if row is None:
                # 未做过 prepare（或 prepare 记录未及落盘），记一条
                # ABORTED 让重复 abort 也幂等；不涉及任何键。
                with self.dbw, self.conn:
                    self.conn.execute(
                        "INSERT OR IGNORE INTO txn_log(tid,state,writes,applies,created_at) "
                        "VALUES(?,?, '{}',0,?)",
                        (tid, ABORTED, time.time()),
                    )
                return ABORTED
            if row["state"] == ABORTED:
                return ABORTED
            if row["state"] == COMMITTED:
                raise RuntimeError("tid=%s ABORT but local state COMMITTED" % tid)
            with self.dbw, self.conn:
                self.conn.execute(
                    "UPDATE txn_log SET state=? WHERE tid=?", (ABORTED, tid)
                )
                self.conn.execute("DELETE FROM locks WHERE tid=?", (tid,))
            log(self.name, "tid=%s ABORTED, locks released" % tid)
            return ABORTED

    # ---- 协调者恢复用：查询决定后的本地状态 -------------------------------

    def decision(self, tid):
        with self.lock:
            row = self._get_log(tid)
            if row is None:
                return {"tid": tid, "state": "UNKNOWN"}
            return {"tid": tid, "state": row["state"]}

    # ---- 自主恢复 ----------------------------------------------------------

    def recover(self):
        """重启恢复。

        PREPARED 事务：绝不自作主张。立即向协调者查询决定；协调者
        暂时不可达则保持 PREPARED（锁也保持），由后台 watcher 轮询。
        COMMITTED/ABORTED 是终态，无需动作（崩溃发生在提交事务
        COMMIT 之前时状态仍是 PREPARED，提交事务本身是原子的，
        不存在“写了一半”的中间态）。
        """
        pending = self._prepared_tids()
        if pending:
            log(self.name, "recovery: %d prepared txn(s) pending: %s"
                % (len(pending), pending))
        for tid in pending:
            self._resolve_one(tid)
        threading.Thread(target=self._prepared_watcher, daemon=True).start()

    def _prepared_tids(self):
        with self.lock:
            cur = self.conn.execute(
                "SELECT tid FROM txn_log WHERE state=? ORDER BY created_at",
                (PREPARED,),
            )
            return [r["tid"] for r in cur.fetchall()]

    def _resolve_one(self, tid):
        """向协调者询问一个已准备事务的决定并执行。协调者不可达则等待。"""
        try:
            resp = http_json(
                "%s/decision/%s" % (self.coord_url, tid), {}, method="GET", timeout=2.0
            )
        except Exception as e:
            log(self.name, "tid=%s coord unreachable (%s); staying PREPARED"
                % (tid, e.__class__.__name__))
            return
        state = resp.get("state")
        if state == COMMITTED:
            self.commit(tid)
        elif state == ABORTED:
            self.abort(tid)
        else:
            # UNKNOWN / PREPARED：协调者也还没结论，继续等。
            log(self.name, "tid=%s coord says %s; staying PREPARED" % (tid, state))

    def _prepared_watcher(self):
        """协调者恢复期间参与者随时可能收到决定；同时兜底轮询那些
        决定通知被错过的 PREPARED 事务。"""
        while True:
            time.sleep(0.5)
            try:
                for tid in self._prepared_tids():
                    self._resolve_one(tid)
            except Exception as e:
                log(self.name, "watcher error: %r" % e)

    # ---- 调试 / 测试用 -----------------------------------------------------

    def debug_state(self):
        with self.lock:
            kv = {r["key"]: r["value"] for r in self.conn.execute(
                "SELECT key,value FROM kv ORDER BY key")}
            txns = [dict(r) for r in self.conn.execute(
                "SELECT tid,state,applies FROM txn_log ORDER BY tid")]
            locks = {r["key"]: r["tid"] for r in self.conn.execute(
                "SELECT key,tid FROM locks ORDER BY key")}
        return {"name": self.name, "kv": kv, "txns": txns, "locks": locks}


# ---- HTTP 服务 -------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    participant = None  # 启动时注入

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
        pass  # 日志走 log()，避免 stderr 噪音

    def do_GET(self):
        p = self.participant
        try:
            if self.path == "/health":
                self._send(200, {"ok": True, "name": p.name})
            elif self.path.startswith("/decision/"):
                tid = self.path.rsplit("/", 1)[-1]
                self._send(200, p.decision(tid))
            elif self.path == "/debug/state":
                self._send(200, p.debug_state())
            else:
                self._send(404, {"error": "not found: %s" % self.path})
        except Exception as e:
            self._send(500, {"error": "%r" % e})

    def do_POST(self):
        p = self.participant
        try:
            body = self._read_json()
            if self.path == "/prepare":
                kind, state = p.prepare(body["tid"], body.get("writes", {}))
                # 应答前阻塞：投票与锁已 fsync 落盘，但响应永远到不了
                # 协调者（模拟节点投票后“半死”）。此时 prepare() 已
                # 返回、库锁已释放，不影响 /debug 等只读观察。
                maybe_block(p.name, "p_vote_resp_block", body.get("tid"))
                self._send(200, {"vote": "yes" if kind == "vote-yes" else "no",
                                 "state": state})
            elif self.path == "/commit":
                state = p.commit(body["tid"])
                maybe_block(p.name, "p_commit_resp_block")
                self._send(200 if state != "UNKNOWN" else 409,
                           {"tid": body["tid"], "state": state})
            elif self.path == "/abort":
                state = p.abort(body["tid"])
                self._send(200, {"tid": body["tid"], "state": state})
            else:
                self._send(404, {"error": "not found: %s" % self.path})
        except KeyError as e:
            self._send(400, {"error": "missing field %s" % e})
        except Exception as e:
            log(p.name, "handler error: %r" % e)
            self._send(500, {"error": "%r" % e})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--coord", required=True, help="协调者 base URL")
    args = ap.parse_args()

    p = Participant(args.name, args.db, args.coord)
    p.recover()

    _Handler.participant = p
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), _Handler)
    log(args.name, "up on port %d db=%s" % (args.port, args.db))
    httpd.serve_forever()


if __name__ == "__main__":
    main()
