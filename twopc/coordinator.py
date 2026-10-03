"""2PC 协调者。

协调者自己也有一份持久事务日志（独立 SQLite 文件）。状态机::

    preparing ──全票 yes──> committing ──全部 ack──> committed
       │                        │
       └──任何 no/失败──────────┴──> aborting ──全部 ack──> aborted

铁律:

1. **决定先落盘再通知** —— ``committing`` / ``aborting`` 记录 fsync
   成功之后才向任何参与者发送 commit/abort；
2. 参与者已 prepare 而协调者暂时不可达时，事务在双方都保持待决、
   锁不释放，参与者不自行猜测；
3. 只有全部参与者都确认提交（提交在各自库上已落盘），才向客户端
   返回 ``committed``；任何拿不准的窗口都返回 ``pending``，绝不假成功；
4. 相同事务 ID 的客户端重试不重复执行：进行中的并发重试直接
   ``pending``，已结束的重试返回终态，未完成的由后台恢复线程推进。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from . import rpc

PREPARING = "preparing"
COMMITTING = "committing"
COMMITTED = "committed"
ABORTING = "aborting"
ABORTED = "aborted"

# 客户端可见的终态
FINAL = {COMMITTED, ABORTED}


class Coordinator:
    def __init__(
        self,
        db_path: str,
        participants: List[Dict[str, Any]],
        crash_plan: Optional[Dict[str, Any]] = None,
        sweep_interval: float = 0.25,
        key_lock_timeout: float = 15.0,
    ) -> None:
        self.db_path = db_path
        self.participants = participants
        self._executor = ThreadPoolExecutor(max_workers=max(4, len(participants)))
        self._crash_triggers: List[Dict[str, str]] = list(
            (crash_plan or {}).get("triggers", [])
        )
        # 每个事务一把内存锁，串行化同一 tid 的并发客户端重试
        self._txn_locks: Dict[str, threading.Lock] = {}
        self._txn_locks_guard = threading.Lock()
        # 协调者是冲突事务的串行化点: key -> 持有该键的 tid。
        # 这把内存锁表与参与者磁盘键锁配合，保证并发冲突事务
        # 以同一个全局顺序进入两阶段提交，避免不同参与者按相反
        # 顺序应用两个都提交的冲突事务（状态分叉）。
        self._key_owner: Dict[str, str] = {}
        self._key_cond = threading.Condition()
        self._key_lock_timeout = key_lock_timeout
        self._stop_sweeper = threading.Event()
        self._init_db()
        self._sweep(lock_all=False)  # 启动恢复
        self._sweeper = threading.Thread(target=self._sweeper_loop,
                                         args=(sweep_interval,), daemon=True)
        self._sweeper.start()

    # ------------------------------------------------------------------ 存储

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS txn_log ("
                "tid TEXT PRIMARY KEY, state TEXT NOT NULL, "
                "ops TEXT NOT NULL, created_at REAL NOT NULL, "
                "updated_at REAL NOT NULL)"
            )
            # 通知进度（哪些参与者已确认终态），供恢复时断点续传
            conn.execute(
                "CREATE TABLE IF NOT EXISTS txn_ack ("
                "tid TEXT NOT NULL, participant TEXT NOT NULL, "
                "PRIMARY KEY(tid, participant))"
            )

    def _crash_if(self, point: str, tid: str) -> None:
        for i, trig in enumerate(self._crash_triggers):
            if trig.get("point") == point and trig.get("tid") == tid:
                del self._crash_triggers[i]
                os._exit(99)

    def _txn_lock(self, tid: str) -> threading.Lock:
        with self._txn_locks_guard:
            lk = self._txn_locks.get(tid)
            if lk is None:
                lk = threading.Lock()
                self._txn_locks[tid] = lk
            return lk

    # ----------------------------------------------------------- 冲突串行化

    @staticmethod
    def _ops_keys(ops: Any) -> List[str]:
        keys = sorted({str(o["key"]) for o in ops})
        return keys

    def _acquire_keys(self, tid: str, keys: List[str],
                      timeout: Optional[float] = None) -> bool:
        """按全局序获取键所有权；拿不到则有限等待。

        返回 False 表示超时仍有键被其它（可能在崩溃恢复中的）事务占用，
        调用方应稍后重试，不能跳过锁继续。``timeout=0`` 为非阻塞。
        """
        if timeout is None:
            timeout = self._key_lock_timeout
        deadline = time.time() + timeout
        with self._key_cond:
            for k in keys:
                while True:
                    owner = self._key_owner.get(k)
                    if owner is None or owner == tid:
                        self._key_owner[k] = tid
                        break
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        return False
                    self._key_cond.wait(timeout=remaining)
            return True

    def _release_keys(self, tid: str, keys: List[str]) -> None:
        with self._key_cond:
            for k in keys:
                if self._key_owner.get(k) == tid:
                    del self._key_owner[k]
            self._key_cond.notify_all()

    def _keys_of_txn(self, tid: str) -> List[str]:
        conn = self._connect()
        try:
            row = self._load(conn, tid)
            return self._ops_keys(json.loads(row[1])) if row else []
        finally:
            conn.close()

    def _load(self, conn: sqlite3.Connection, tid: str):
        return conn.execute(
            "SELECT state, ops FROM txn_log WHERE tid=?", (tid,)
        ).fetchone()

    def _save_state(self, conn: sqlite3.Connection, tid: str, state: str) -> None:
        conn.execute(
            "UPDATE txn_log SET state=?, updated_at=? WHERE tid=?",
            (state, time.time(), tid),
        )

    def _acked(self, conn: sqlite3.Connection, tid: str) -> set:
        return {
            r[0]
            for r in conn.execute(
                "SELECT participant FROM txn_ack WHERE tid=?", (tid,)
            )
        }

    # -------------------------------------------------------- 参与者 RPC 扇出

    def _broadcast(
        self, method: str, payload: Dict[str, Any], need_acks: Optional[set] = None
    ) -> Dict[str, Dict[str, Any]]:
        """并行扇出；返回 ``{participant_name: {'ok': bool, ...}}``。

        ``need_acks`` 非 None 时只通知尚未确认的参与者。
        """
        targets = self.participants
        if need_acks is not None:
            targets = [p for p in self.participants if p["name"] not in need_acks]

        def one(p: Dict[str, Any]) -> Dict[str, Any]:
            addr = (p["host"], p["port"])
            try:
                result = rpc.call(addr, method, payload)
                return {"ok": True, "result": result}
            except rpc.RPCError as exc:
                return {"ok": False, "error": str(exc)}

        futures = {
            p["name"]: self._executor.submit(one, p) for p in targets
        }
        return {name: fut.result() for name, fut in futures.items()}

    # --------------------------------------------------------------- 主流程

    def submit(
        self, tid: str, ops: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        lk = self._txn_lock(tid)
        if not lk.acquire(blocking=False):
            # 相同 tid 的并发重试：另一个线程正在推进，让客户端等待/重试，
            # 绝不重复执行 prepare/commit。
            return {"tid": tid, "state": "pending", "reason": "in-flight retry"}
        try:
            return self._run_transaction(tid, ops)
        finally:
            lk.release()

    def _run_transaction(
        self, tid: str, ops: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        keys = self._ops_keys(ops)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._load(conn, tid)
            if row is not None:
                # 已见过的 tid（客户端重试 / 重启后推进）：按日志状态走
                state = row[0]
                conn.commit()
            else:
                # 新事务：协调者作为冲突串行化点，先拿全部键的所有权
                conn.rollback()
                if not self._acquire_keys(tid, keys):
                    return {
                        "tid": tid,
                        "state": "pending",
                        "reason": "keys busy; retry later",
                        "waiting_keys": [
                            k for k in keys
                            if self._key_owner.get(k) not in (None, tid)
                        ],
                    }
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    row2 = self._load(conn, tid)
                    if row2 is not None:
                        # 等待键锁期间恢复线程可能已处理该 tid
                        state = row2[0]
                        conn.commit()
                    else:
                        # 先把“开始两阶段提交”记入日志
                        conn.execute(
                            "INSERT INTO txn_log(tid, state, ops, "
                            "created_at, updated_at) VALUES(?,?,?,?,?)",
                            (tid, PREPARING, json.dumps(ops),
                             time.time(), time.time()),
                        )
                        conn.commit()
                        state = PREPARING
                except BaseException:
                    self._release_keys(tid, keys)
                    raise
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

        if row is not None or state != PREPARING:
            if state in FINAL:
                return {"tid": tid, "state": state, "duplicate": True}
            if state == COMMITTING:
                return self._finish_commit(tid)
            if state == ABORTING:
                return self._finish_abort(tid)
            # preparing 残留行（协调者在 prepare 期间崩溃过）：推定中止
            return self._abort_from_preparing(tid, ops)

        # ---------- 阶段 1：prepare（投票） ----------
        self._crash_if("coordinator:prepare:after", tid)
        votes = self._broadcast("prepare", {"tid": tid, "ops": ops})
        yes = all(v.get("ok") and v["result"].get("vote") == "yes"
                  for v in votes.values()) and len(votes) == len(self.participants)

        if not yes:
            return self._abort_after_vote(tid, ops)
        return self._commit_after_vote(tid, ops)

    def _persist_decision(self, tid: str, state: str) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE txn_log SET state=?, updated_at=? WHERE tid=?",
                (state, time.time(), tid),
            )
            conn.commit()  # 决定落盘（WAL+FULL，返回即 fsync）
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _commit_after_vote(self, tid: str, ops: Any) -> Dict[str, Any]:
        # 决定“提交”先落盘，然后才通知任何参与者
        self._crash_if("coordinator:commit:decision:before", tid)
        self._persist_decision(tid, COMMITTING)
        self._crash_if("coordinator:commit:decision:after", tid)
        return self._finish_commit(tid)

    def _finish_commit(self, tid: str) -> Dict[str, Any]:
        self._crash_if("coordinator:commit:notify:before", tid)
        conn = self._connect()
        try:
            acked = self._acked(conn, tid)
        finally:
            conn.close()
        results = self._broadcast(
            "commit", {"tid": tid}, need_acks=acked
        )
        self._crash_if("coordinator:commit:notify:after", tid)

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            for name, res in results.items():
                if res.get("ok"):
                    conn.execute(
                        "INSERT OR IGNORE INTO txn_ack(tid, participant) "
                        "VALUES(?,?)",
                        (tid, name),
                    )
            acked = self._acked(conn, tid)
            all_acked = acked == {p["name"] for p in self.participants}
            if all_acked:
                self._save_state(conn, tid, COMMITTED)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

        if all_acked:
            self._release_keys(tid, self._keys_of_txn(tid))
            return {"tid": tid, "state": COMMITTED}
        # 拿不准（参与者宕机/拒绝）：保持待决，绝不返回成功；键继续持有，
        # 由 sweeper 在参与者恢复后补完并释放。
        return {
            "tid": tid,
            "state": "pending",
            "waiting_on": sorted(
                {p["name"] for p in self.participants} - acked
            ),
        }

    def _abort_after_vote(self, tid: str, ops: Any) -> Dict[str, Any]:
        # prepare 阶段有 No 票/不可达：决定“中止”先落盘再通知
        self._crash_if("coordinator:abort:decision:before", tid)
        self._persist_decision(tid, ABORTING)
        self._crash_if("coordinator:abort:decision:after", tid)
        return self._finish_abort(tid, ops)

    def _abort_from_preparing(self, tid: str, ops: Any) -> Dict[str, Any]:
        # 重启恢复：没有持久的提交决定 => 推定中止（presumed abort）。
        # 协调者从未落盘过 commit 决定，任何 prepared 的参与者都只能等待。
        self._persist_decision(tid, ABORTING)
        return self._finish_abort(tid, ops)

    def _finish_abort(self, tid: str, ops: Any = None) -> Dict[str, Any]:
        if ops is None:
            conn = self._connect()
            try:
                row = self._load(conn, tid)
                ops = json.loads(row[1]) if row else []
            finally:
                conn.close()

        self._crash_if("coordinator:abort:notify:before", tid)
        conn = self._connect()
        try:
            acked = self._acked(conn, tid)
        finally:
            conn.close()
        results = self._broadcast(
            "abort", {"tid": tid, "ops": ops}, need_acks=acked
        )
        self._crash_if("coordinator:abort:notify:after", tid)

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            for name, res in results.items():
                # abort 收到参与者的任何已持久化应答都算确认；
                # UNKNOWN_TXN 之类不会出现（abort 允许补记），
                # 只有连接级失败才重试。
                if res.get("ok"):
                    conn.execute(
                        "INSERT OR IGNORE INTO txn_ack(tid, participant) "
                        "VALUES(?,?)",
                        (tid, name),
                    )
            acked = self._acked(conn, tid)
            all_acked = acked == {p["name"] for p in self.participants}
            if all_acked:
                self._save_state(conn, tid, ABORTED)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

        # 中止决定已持久：不存在任何参与者日后提交的可能（presumed abort），
        # 因此对客户端可以给出确定的 aborted；但键所有权必须等所有
        # 参与者确认中止（释放各自磁盘锁）后才交还，否则下一个冲突事务
        # 可能在某个还没处理完 abort 的参与者处拿到不一致的顺序。
        if all_acked:
            self._release_keys(tid, self._keys_of_txn(tid))
        return {"tid": tid, "state": ABORTED}

    # ----------------------------------------------------------- 恢复/扫描

    def _sweeper_loop(self, interval: float) -> None:
        while not self._stop_sweeper.wait(interval):
            try:
                self._sweep(lock_all=True)
            except Exception:
                pass  # 扫描失败下个周期再试，不影响主路径

    def _sweep(self, lock_all: bool) -> None:
        """按日志完成恢复:

        * ``committing`` 但缺 ack → 继续通知 commit；
        * ``aborting`` 但缺 ack → 继续通知 abort；
        * 启动时（``lock_all=False``）把残留 ``preparing`` 推定为中止。

        恢复线程也会把事务涉及的键所有权接过来（非阻塞获取；被其它
        未完成事务占用则下周期再试），完成后在 ``_finish_*`` 中释放。
        """
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT tid, state, ops FROM txn_log"
            ).fetchall()
        finally:
            conn.close()

        for tid, state, ops_json in rows:
            if state not in (COMMITTING, ABORTING) and not (
                not lock_all and state == PREPARING
            ):
                continue
            lk = self._txn_lock(tid)
            if not lk.acquire(blocking=False):
                # 正有客户端线程在处理该 tid，跳过
                continue
            try:
                ops = json.loads(ops_json)
                keys = self._ops_keys(ops)
                if not self._acquire_keys(tid, keys, timeout=0):
                    # 与其它待恢复事务存在键冲突，按日志顺序下周期再推进
                    continue
                if state == COMMITTING:
                    self._finish_commit(tid)
                elif state == ABORTING:
                    self._finish_abort(tid, ops)
                else:
                    self._abort_from_preparing(tid, ops)
            except Exception:
                pass
            finally:
                lk.release()

    # ------------------------------------------------------------- 查询/RPC

    def status(self, tid: str) -> Dict[str, Any]:
        conn = self._connect()
        try:
            row = self._load(conn, tid)
            if row is None:
                return {"tid": tid, "state": "unknown"}
            state = row[0]
            if state == COMMITTING:
                state = "pending"  # 决定已落盘但未全部确认
            acked = sorted(self._acked(conn, tid))
            return {
                "tid": tid,
                "state": state,
                "acked_by": acked,
                "waiting_on": sorted(
                    {p["name"] for p in self.participants} - set(acked)
                )
                if state not in FINAL
                else [],
            }
        finally:
            conn.close()

    def dump(self) -> Dict[str, Any]:
        conn = self._connect()
        try:
            txns = [
                {"tid": t, "state": s}
                for t, s in conn.execute(
                    "SELECT tid, state FROM txn_log ORDER BY created_at"
                )
            ]
            acks = [
                {"tid": t, "participant": p}
                for t, p in conn.execute(
                    "SELECT tid, participant FROM txn_ack ORDER BY tid"
                )
            ]
            return {"txns": txns, "acks": acks}
        finally:
            conn.close()

    def handle(
        self, method: str, params: Dict[str, Any], addr: tuple
    ) -> Dict[str, Any]:
        if method == "submit":
            return self.submit(params["tid"], params["ops"])
        if method == "status":
            return self.status(params["tid"])
        if method == "dump":
            return self.dump()
        raise rpc.RPCError(f"unknown method {method}")
