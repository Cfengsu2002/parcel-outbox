import argparse
import json
import os
import signal
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path

from parcel_outbox.deadline import DeadlineDelivery
from parcel_outbox.inspection import STATES, inspect_events
from parcel_outbox.store import Outbox
from parcel_outbox.transport import HttpDelivery
from parcel_outbox.worker import Worker


def status(path):
    # 只读状态检查不能隐式创建数据库，也不返回事件正文或用户键。
    uri = Path(path).resolve().as_uri() + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True)) as db:
        row = db.execute('''SELECT count(*),
            coalesce(sum(delivered=1),0), coalesce(sum(dead=1),0),
            coalesce(sum(delivered=0 AND dead=0),0) FROM events''').fetchone()
        return dict(zip(('total', 'delivered', 'dead', 'pending'), row))


def parser():
    root = argparse.ArgumentParser(description='Durable SQLite webhook outbox')
    root.add_argument('--db', required=True, help='local SQLite database path')
    commands = root.add_subparsers(dest='command', required=True)
    enqueue = commands.add_parser('enqueue', help='read JSON payload from stdin')
    enqueue.add_argument('--key', required=True)
    commands.add_parser('status', help='show aggregate counts without event data')
    inspect = commands.add_parser('inspect', help='page through event metadata without payloads')
    inspect.add_argument('--after', type=int, default=0)
    inspect.add_argument('--through', type=int)
    inspect.add_argument('--limit', type=int, default=100)
    inspect.add_argument('--state', choices=STATES)
    replay = commands.add_parser('replay', help='reset one dead event for delivery')
    replay.add_argument('event_id', type=int)
    worker = commands.add_parser('work', help='deliver queued events')
    worker.add_argument('--endpoint', required=True)
    worker.add_argument('--owner', default='parcel-worker')
    worker.add_argument('--deadline', type=float, default=10)
    worker.add_argument('--lease', type=float, default=30)
    worker.add_argument('--max-attempts', type=int, default=5)
    worker.add_argument('--once', action='store_true')
    worker.add_argument('--allow-loopback-http', action='store_true')
    return root


def emit(value):
    print(json.dumps(value, sort_keys=True), flush=True)


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == 'status':
            emit(status(args.db))
        elif args.command == 'inspect':
            emit(inspect_events(args.db, args.after, args.limit, args.state, args.through))
        elif args.command == 'enqueue':
            raw = sys.stdin.buffer.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError('input too large')
            payload = json.loads(raw)
            emit({'event_id': Outbox(args.db).enqueue(args.key, payload)})
        elif args.command == 'replay':
            if not Path(args.db).is_file():
                raise ValueError('database does not exist')
            Outbox(args.db).replay(args.event_id, time.time())
            emit({'replayed': args.event_id})
        else:
            key_text = os.environ.get('PARCEL_SIGNING_KEY_HEX', '')
            if len(key_text) > 512:
                raise ValueError('signing key too large')
            sender = HttpDelivery(args.endpoint, bytes.fromhex(key_text),
                                  allow_loopback_http=args.allow_loopback_http)
            transport = DeadlineDelivery(sender, args.deadline)
            worker = Worker(None, transport, args.owner, lease_seconds=args.lease,
                            max_attempts=args.max_attempts)
            worker.store = Outbox(args.db)
            previous = {}
            try:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    previous[sig] = signal.signal(sig, lambda number, frame: worker.stop())
                if args.once:
                    emit({'state': worker.run_once()})
                else:
                    emit({'state': 'started'})
                    worker.run()
                    emit({'state': 'stopped'})
            finally:
                for sig, handler in previous.items():
                    signal.signal(sig, handler)
    except (ValueError, sqlite3.Error, OSError, RuntimeError):
        # 不向终端回显端点、环境变量、载荷或底层异常详情。
        print('Operation failed; check input, database access and signing configuration.', file=sys.stderr)
        return 1
    return 0
