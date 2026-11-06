import argparse
import json
import os
import re
import sqlite3
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from parcel_outbox.receiver import InvalidDelivery, Receiver


def make_handler(receiver, apply, clock=time.time):
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def reply(self, code):
            body = json.dumps({'success': code == 200}).encode('ascii')
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

        def do_POST(self):
            if self.path != '/hook':
                self.reply(404)
                return
            # 拒绝重复安全头和不支持的分块编码，避免代理与应用解释不一致。
            required = ('Content-Length', 'Content-Type', 'Parcel-Timestamp', 'Parcel-Signature')
            if self.headers.get_all('Transfer-Encoding') or any(len(self.headers.get_all(h, [])) != 1 for h in required):
                self.reply(400)
                return
            length = self.headers['Content-Length']
            if re.fullmatch(r'0|[1-9][0-9]{0,9}', length) is None:
                self.reply(400)
                return
            length = int(length)
            if length > receiver.max_body_bytes:
                self.reply(413)
                return
            if self.headers['Content-Type'].split(';')[0].strip().lower() != 'application/json':
                self.reply(415)
                return
            try:
                body = self.rfile.read(length)
                if len(body) != length:
                    self.reply(400)
                    return
                receiver.receive(body, self.headers['Parcel-Timestamp'],
                                 self.headers['Parcel-Signature'], clock(), apply)
            except InvalidDelivery:
                self.reply(400)
                return
            except (OSError, sqlite3.Error, RuntimeError):
                self.reply(503)
                return
            self.reply(200)

        def log_message(self, *args):
            # 演示服务不记录请求路径、签名、载荷或异常详情。
            pass

    return Handler


def main():
    parser = argparse.ArgumentParser(description='Local-only signed receiver demonstration')
    parser.add_argument('--db', required=True)
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    key = bytes.fromhex(os.environ.get('PARCEL_SIGNING_KEY_HEX', ''))
    receiver = Receiver(args.db, [key])
    with closing(sqlite3.connect(args.db)) as db, db:
        db.execute('CREATE TABLE IF NOT EXISTS applied_events(event_key TEXT PRIMARY KEY)')

    def apply(db, key, payload):
        db.execute('INSERT INTO applied_events VALUES(?)', (key,))

    server = ThreadingHTTPServer(('127.0.0.1', args.port), make_handler(receiver, apply))
    server.daemon_threads = True
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
