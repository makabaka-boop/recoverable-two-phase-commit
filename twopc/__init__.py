"""Two-Phase Commit demo: coordinator + independent-SQLite participants.

模块划分:

* :mod:`twopc.rpc`          —— 行分隔 JSON 的 TCP RPC
* :mod:`twopc.participant`  —— 参与者（独立 SQLite 文件、投票日志、键锁）
* :mod:`twopc.coordinator`  —— 协调者（事务日志、决定落盘后才通知）
"""

__all__ = ["participant", "coordinator", "rpc"]
