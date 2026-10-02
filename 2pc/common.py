"""2PC 公共工具：结构化日志、崩溃注入、JSON-over-HTTP RPC。

崩溃点(crash point)通过环境变量配置，格式：

    CRASH=<点1>,<点2>,...

也可带计数后缀，表示该点第 N 次命中时崩溃（默认 1，一次性）：

    CRASH=coord_decision_after:2

进程被 os._exit(9) 直接杀死，模拟宕机；磁盘上的 SQLite 文件保持
崩溃前最近一次 fsync 后的一致状态。
"""

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

# ---- 日志 ------------------------------------------------------------------

_log_lock = threading.Lock()


def log(role, msg):
    line = "[%s %s] %s" % (
        time.strftime("%H:%M:%S"),
        role,
        msg,
    )
    with _log_lock:
        print(line, file=sys.stderr, flush=True)


# ---- 崩溃注入 --------------------------------------------------------------

_crash_lock = threading.Lock()


def crash_points():
    """解析 CRASH 环境变量 -> {点名: 命中次数阈值}。"""
    spec = os.environ.get("CRASH", "").strip()
    pts = {}
    if spec:
        for item in spec.split(","):
            item = item.strip()
            if not item:
                continue
            if ":" in item:
                name, n = item.rsplit(":", 1)
                pts[name.strip()] = int(n)
            else:
                pts[item] = 1
    return pts


def maybe_crash(role, point):
    """命中断点时立即退出进程。

    每个断点带一个命中计数：达到配置阈值才崩溃，且只崩溃一次。
    崩溃发生在调用处 —— 设计上崩溃点紧邻 fsync/网络动作的前后，
    因此“崩溃前”与“崩溃后”语义明确。
    """
    with _crash_lock:
        pts = getattr(maybe_crash, "_pts", None)
        if pts is None:
            pts = crash_points()
            setattr(maybe_crash, "_pts", pts)
        if point not in pts:
            return
        seen = getattr(maybe_crash, "_seen", {})
        seen[point] = seen.get(point, 0) + 1
        setattr(maybe_crash, "_seen", seen)
        fire = seen[point] >= pts[point]
    if fire:
        log(role, "!!! CRASH INJECTED at %s (exit 9)" % point)
        # os._exit 不刷新任何用户态缓冲、不执行 finally/atexit，
        # 只保留已经 fsync 落盘的内容。
        os._exit(9)


# ---- 阻塞注入（模拟节点在某点永久挂起，不丢盘上状态）---------------------

def block_points():
    """解析 BLOCK 环境变量 -> {点名: 第几次命中起阻塞}。

    语法同 CRASH：可带 :N 后缀，默认 1（第一次命中即永久阻塞）。
    """
    spec = os.environ.get("BLOCK", "").strip()
    pts = {}
    if spec:
        for item in spec.split(","):
            item = item.strip()
            if not item:
                continue
            if ":" in item:
                name, n = item.rsplit(":", 1)
                pts[name.strip()] = int(n)
            else:
                pts[item] = 1
    return pts


def maybe_block(role, point, tid=None):
    """命中（达到配置次数）后当前线程永久睡眠（节点进程仍存活、
    日志保持原状）。支持 :N 表示第 N 次命中才阻塞。

    若设置了环境变量 BLOCK_TID，则只有该事务的请求才会触发阻塞
    （恢复线程可能用同一断点重放其他/同一 tid 的请求）。"""
    pts = getattr(maybe_block, "_pts", None)
    if pts is None:
        pts = block_points()
        setattr(maybe_block, "_pts", pts)
    if point not in pts:
        return
    wanted_tid = os.environ.get("BLOCK_TID")
    if wanted_tid and tid != wanted_tid:
        return
    seen = getattr(maybe_block, "_seen", {})
    key = (point, tid or "-")
    seen[key] = seen.get(key, 0) + 1
    setattr(maybe_block, "_seen", seen)
    if seen[key] < pts[point]:
        return
    logged = getattr(maybe_block, "_logged", set())
    if key not in logged:
        logged.add(key)
        setattr(maybe_block, "_logged", logged)
        log(role, "~~~ BLOCKED forever at %s tid=%s (hit %d)"
            % (point, tid, seen[key]))
    while True:
        time.sleep(3600)


# ---- JSON HTTP RPC ---------------------------------------------------------

class RpcError(Exception):
    def __init__(self, message, status=None, body=None):
        super().__init__(message)
        self.status = status
        self.body = body


def http_json(url, payload=None, method=None, timeout=3.0):
    """发起 JSON HTTP 调用。payload 为 None 时发 GET。"""
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
        method = method or "POST"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            body = json.loads(raw)
        except Exception:
            body = {"error": raw}
        raise RpcError(body.get("error", "HTTP %s" % e.code), e.code, body)
