import hashlib
import hmac
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

from parcel_outbox.receiver import InvalidDelivery, Receiver


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'receiver.db'
        self.old = b'o' * 32
        self.new = b'n' * 32
        self.receiver = Receiver(self.path, [self.new, self.old])
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE totals(id TEXT PRIMARY KEY, amount INTEGER NOT NULL)')

    def body(self, payload=None):
        return json.dumps({'event_key': 'order', 'payload': payload or {'amount': 12}}).encode()

    def sign(self, body, timestamp='100', key=None):
        return 'v1=' + hmac.new(key or self.new, timestamp.encode() + b'.' + body, hashlib.sha256).hexdigest()

    def apply(self, db, key, payload):
        db.execute('INSERT INTO totals VALUES(?,?)', (key, payload['amount']))

    def test_rotation_and_replay_window_boundaries(self):
        body = self.body()
        for key in (self.old, self.new):
            for now in (0, 100, 400):
                self.assertEqual('order', self.receiver.verify(body, '100', self.sign(body, key=key), now)['event_key'])
        for now in (-201, 401):
            with self.assertRaises(InvalidDelivery):
                self.receiver.verify(body, '100', self.sign(body), now)
        retired = Receiver(self.path, [self.new])
        with self.assertRaises(InvalidDelivery):
            retired.verify(body, '100', self.sign(body, key=self.old), 100)

    def test_tamper_and_malformed_headers_rejected_before_callback(self):
        body = self.body()
        for raw, stamp, sig in [(body+b' ', '100', self.sign(body)),
                                (body, '101', self.sign(body)), (body, '0100', self.sign(body)),
                                (body, '100', 'v1=broken'), (body, '100', self.sign(body, key=b'x'*32))]:
            with self.subTest(stamp=stamp):
                with self.assertRaises(InvalidDelivery):
                    self.receiver.receive(raw, stamp, sig, 100, self.apply)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(0, db.execute('SELECT count(*) FROM parcel_inbox').fetchone()[0])

    def test_signed_invalid_json_and_oversize_are_rejected(self):
        for body in [b'[]', b'{"event_key":"a"}', b'{"event_key":"a","payload":NaN}',
                     b'{"event_key":"a","payload":1e999}',
                     b'{"event_key":"a","payload":1,"payload":2}', b'\xff']:
            with self.subTest(body=body):
                with self.assertRaises(InvalidDelivery):
                    self.receiver.verify(body, '100', self.sign(body), 100)
        small = Receiver(self.path, [self.new], max_body_bytes=2)
        with self.assertRaises(InvalidDelivery):
            small.verify(self.body(), '100', self.sign(self.body()), 100)

    def test_duplicate_survives_restart_and_new_delivery_timestamp(self):
        body = self.body()
        self.assertTrue(self.receiver.receive(body, '100', self.sign(body), 100, self.apply))
        receiver = Receiver(self.path, [self.new])
        self.assertFalse(receiver.receive(body, '200', self.sign(body, '200'), 200, self.apply))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual([('order', 12)], db.execute('SELECT * FROM totals').fetchall())
        different = self.body({'amount': 99})
        with self.assertRaises(InvalidDelivery):
            receiver.receive(different, '200', self.sign(different, '200'), 200, self.apply)

    def test_callback_failure_rolls_back_both_receipt_and_business_write(self):
        body = self.body()
        def fail(db, key, payload):
            self.apply(db, key, payload)
            raise RuntimeError('simulated failure')
        with self.assertRaises(RuntimeError):
            self.receiver.receive(body, '100', self.sign(body), 100, fail)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(0, db.execute('SELECT count(*) FROM totals').fetchone()[0])
            self.assertEqual(0, db.execute('SELECT count(*) FROM parcel_inbox').fetchone()[0])
        self.assertTrue(self.receiver.receive(body, '100', self.sign(body), 100, self.apply))

    def test_concurrent_deliveries_apply_business_effect_once(self):
        body = self.body()
        def deliver(_):
            receiver = Receiver(self.path, [self.new])
            return receiver.receive(body, '100', self.sign(body), 100, self.apply)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(deliver, range(8)))
        self.assertEqual(1, sum(results))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(1, db.execute('SELECT count(*) FROM totals').fetchone()[0])
