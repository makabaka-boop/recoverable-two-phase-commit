"""进程入口:

参与者::

    python -m twopc participant --name p1 --host 127.0.0.1 --port 9101 \
        --db /tmp/p1.db [--crash /tmp/p1.crash.json]

协调者::

    python -m twopc coordinator --host 127.0.0.1 --port 9200 \
        --db /tmp/coord.db --participants p1=127.0.0.1:9101,... \
        [--crash /tmp/coord.crash.json]

启动成功后向 stdout 打印 ``READY <host> <port>`` 并 flush，
之后只接受 RPC；命中崩溃开关时进程以退出码 99 立即退出。
崩溃开关文件形如::

    {"triggers": [{"point": "participant:vote:after", "tid": "t1"}]}
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List

from . import coordinator as coord_mod
from . import participant as part_mod
from . import rpc


def _load_crash(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_participants(spec: str) -> List[Dict[str, Any]]:
    out = []
    for item in spec.split(","):
        name, hp = item.split("=", 1)
        host, port = hp.split(":")
        out.append({"name": name, "host": host, "port": int(port)})
    return out


def main(argv: List[str] = None) -> None:
    parser = argparse.ArgumentParser(prog="twopc")
    sub = parser.add_subparsers(dest="role", required=True)

    p = sub.add_parser("participant")
    p.add_argument("--name", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--db", required=True)
    p.add_argument("--crash", default=None)

    c = sub.add_parser("coordinator")
    c.add_argument("--host", default="127.0.0.1")
    c.add_argument("--port", type=int, required=True)
    c.add_argument("--db", required=True)
    c.add_argument("--participants", required=True)
    c.add_argument("--crash", default=None)
    c.add_argument("--sweep-interval", type=float, default=0.25)

    args = parser.parse_args(argv)

    if args.role == "participant":
        crash_plan = _load_crash(args.crash) if args.crash else None
        node = part_mod.Participant(args.name, args.db, crash_plan)
        srv = rpc.serve(args.host, args.port, node.handle)
    else:
        crash_plan = _load_crash(args.crash) if args.crash else None
        participants = _parse_participants(args.participants)
        node = coord_mod.Coordinator(
            args.db, participants, crash_plan, args.sweep_interval
        )
        srv = rpc.serve(args.host, args.port, node.handle)

    host, port = srv.getsockname()
    print(f"READY {host} {port}", flush=True)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    import threading

    main()
