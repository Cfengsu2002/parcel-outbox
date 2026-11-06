import http.client
import json
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from http.server import ThreadingHTTPServer
from pathlib import Path

from examples.receiver import make_handler
from parcel_outbox.receiver import Receiver
from parcel_outbox.transport import HttpDelivery


class ReceiverHttpTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'receiver.db'
        self.receiver = Receiver(self.path, [b'k' * 32], max_body_bytes=1024)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE applied(event_key TEXT PRIMARY KEY)')
        def apply(db, key, payload):
            db.execute('INSERT INTO applied VALUES(?)', (key,))
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(self.receiver, apply))
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.cleanup_server)

    def cleanup_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_real_transport_duplicate_delivers_once(self):
        transport = HttpDelivery(f'http://127.0.0.1:{self.server.server_port}/hook',
                                 b'k' * 32, allow_loopback_http=True)
        for _ in range(2):
            self.assertTrue(transport.send('same', {'amount': 1}, int(time.time())).delivered)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual([('same',)], db.execute('SELECT * FROM applied').fetchall())
            self.assertEqual(1, db.execute('SELECT count(*) FROM parcel_inbox').fetchone()[0])

    def raw_request(self, headers, body=b''):
        with socket.create_connection(('127.0.0.1', self.server.server_port), timeout=2) as connection:
            connection.sendall(b'POST /hook HTTP/1.1\r\nHost: localhost\r\n' + headers + b'\r\n' + body)
            return connection.recv(1024).split(b'\r\n')[0]

    def test_oversize_rejected_without_waiting_for_body(self):
        headers = (b'Content-Length: 9999\r\nContent-Type: application/json\r\n'
                   b'Parcel-Timestamp: 1\r\nParcel-Signature: v1=bad\r\n')
        self.assertIn(b'413', self.raw_request(headers))

    def test_duplicate_signature_or_length_and_chunked_rejected(self):
        base = (b'Content-Length: 0\r\nContent-Type: application/json\r\n'
                b'Parcel-Timestamp: 1\r\nParcel-Signature: v1=bad\r\n')
        for extra in (b'Parcel-Signature: v1=other\r\n', b'Content-Length: 0\r\n',
                      b'Transfer-Encoding: chunked\r\n'):
            self.assertIn(b'400', self.raw_request(base + extra))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(0, db.execute('SELECT count(*) FROM parcel_inbox').fetchone()[0])

    def test_invalid_signature_and_wrong_content_type_rejected(self):
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=2)
        try:
            for content_type, expected in [('application/json', 400), ('text/plain', 415)]:
                connection.request('POST', '/hook', b'{}', {'Content-Type': content_type,
                                   'Parcel-Timestamp': '1', 'Parcel-Signature': 'bad'})
                response = connection.getresponse()
                self.assertEqual(expected, response.status)
                self.assertEqual({'success': False}, json.loads(response.read()))
        finally:
            connection.close()
