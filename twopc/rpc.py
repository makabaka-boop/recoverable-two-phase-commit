"""行分隔 JSON 的极简 RPC。

协议: 每条请求/响应都是单行 JSON，以 ``\\n`` 结束；一个连接处理
一条请求（``Connection: close`` 风格），简单且天然满足幂等重试。

服务方法约定: ``handler(method, params, addr) -> dict``；
处理方法内部主动 ``raise Exception`` 会被转成 ``{"error": ...}``，
连接失败/对端崩溃则在客户端侧表现为 :class:`RPCError`。
"""

from __future__ import annotations

import json
import socket
import threading
from typing import Any, Callable, Dict, Optional

Handler = Callable[[str, Dict[str, Any], tuple], Dict[str, Any]]


class RPCError(Exception):
    """所有 RPC 层错误的基类。"""


def serve(host: str, port: int, handler: Handler) -> socket.socket:
    """在 ``host:port`` 上启动一个常驻 RPC 服务循环（当前线程内 accept）。

    返回监听 socket（测试可读取实际绑定端口）。
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(64)

    def loop() -> None:
        while True:
            try:
                conn, addr = srv.accept()
            except OSError:
                return
            threading.Thread(
                target=_handle, args=(conn, addr, handler), daemon=True
            ).start()

    threading.Thread(target=loop, daemon=True).start()
    return srv


def _handle(conn: socket.socket, addr: tuple, handler: Handler) -> None:
    try:
        with conn:
            f = conn.makefile("rwb", buffering=0)
            line = f.readline()
            if not line:
                return
            try:
                req = json.loads(line.decode("utf-8"))
                result = handler(req["method"], req.get("params") or {}, addr)
                resp = {"ok": True, "result": result}
            except Exception as exc:  # 业务异常 -> 错误响应
                resp = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            f.write((json.dumps(resp, ensure_ascii=False) + "\n").encode("utf-8"))
    except (OSError, ConnectionError):
        pass


def call(
    address: tuple,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 5.0,
) -> Dict[str, Any]:
    """同步发起一次 RPC。业务错误抛 :class:`RPCError`。"""
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.settimeout(timeout)
    try:
        conn.connect(address)
        f = conn.makefile("rwb", buffering=0)
        f.write(
            (json.dumps({"method": method, "params": params or {}}) + "\n").encode(
                "utf-8"
            )
        )
        line = f.readline()
    except (OSError, socket.timeout) as exc:
        raise RPCError(f"call {method} on {address} failed: {exc}") from exc
    finally:
        conn.close()

    if not line:
        raise RPCError(f"call {method} on {address}: empty response (crashed?)")
    resp = json.loads(line.decode("utf-8"))
    if not resp.get("ok"):
        raise RPCError(resp.get("error", "unknown error"))
    return resp["result"]
