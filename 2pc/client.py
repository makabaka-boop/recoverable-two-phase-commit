"""命令行客户端：向协调者提交一个事务，或查询事务状态。

提交格式（每个 name:k=v 为一个参与者写入一对键值，可重复）：

    python3 client.py --coord http://127.0.0.1:17000 submit T1 \
        --put pa:x=1 --put pa:y=2 --put pb:z=3
    python3 client.py --coord http://127.0.0.1:17000 status T1

退出码：0=committed，2=aborted，3=pending（无法确定，稍后可用同一
tid 重试，绝无副作用），1=错误。
"""

import argparse
import sys
import time

from common import RpcError, http_json


def submit(coord, tid, puts, poll=0.0):
    writes = {}
    for item in puts:
        try:
            target, kv = item.split(":", 1)
            key, value = kv.split("=", 1)
        except ValueError:
            raise SystemExit("bad --put %r, expected participant:key=value"
                             % item)
        writes.setdefault(target, {})[key] = value
    resp = http_json(coord.rstrip("/") + "/transact",
                     {"tid": tid, "writes": writes}, timeout=10.0)
    state = resp["state"]
    deadline = time.time() + poll
    while state == "pending" and time.time() < deadline:
        time.sleep(0.3)
        resp = http_json("%s/status/%s" % (coord.rstrip("/"), tid),
                         None, timeout=5.0)
        st = resp["state"]
        state = {"COMMITTED": "committed", "ABORTED": "aborted"}.get(
            st, "pending" if st == "STARTED" else st)
    return state


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coord", default="http://127.0.0.1:17000")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("submit")
    s.add_argument("tid")
    s.add_argument("--put", dest="puts", action="append", default=[],
                   help="participant:key=value，可多次给出")
    s.add_argument("--poll", type=float, default=0.0,
                   help="pending 时最多轮询多少秒")

    q = sub.add_parser("status")
    q.add_argument("tid")

    args = ap.parse_args()

    try:
        if args.cmd == "submit":
            state = submit(args.coord, args.tid, args.puts, args.poll)
        else:
            resp = http_json("%s/status/%s" % (args.coord.rstrip("/"), args.tid),
                             None, timeout=5.0)
            print(resp)
            sys.exit(0 if resp["state"] != "unknown" else 3)
    except RpcError as e:
        print("rpc error: %s %s" % (e.status, e.body))
        sys.exit(1)
    except OSError as e:
        print("coordinator unreachable: %r" % e)
        sys.exit(1)

    print(state)
    sys.exit({"committed": 0, "aborted": 2, "pending": 3}[state])


if __name__ == "__main__":
    main()
