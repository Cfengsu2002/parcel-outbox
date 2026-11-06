import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from parcel_outbox.store import Outbox


class TransactionTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Outbox(Path(directory.name) / 'events.db')
        with self.store.transaction() as db:
            db.execute('CREATE TABLE orders(id TEXT PRIMARY KEY, amount INTEGER NOT NULL)')

    def test_business_write_and_event_rollback_together(self):
        with self.assertRaises(RuntimeError):
            with self.store.transaction() as db:
                db.execute('INSERT INTO orders VALUES(?,?)', ('first', 10))
                self.store.enqueue_in(db, 'order:first', {'amount': 10})
                raise RuntimeError('simulated crash before commit')
        with self.store.transaction() as db:
            self.assertEqual(0, db.execute('SELECT count(*) FROM orders').fetchone()[0])
        self.assertIsNone(self.store.claim('worker', 0))

    def test_atomic_commit_visible_only_after_transaction_finishes(self):
        with self.store.transaction() as db:
            db.execute('INSERT INTO orders VALUES(?,?)', ('first', 10))
            self.store.enqueue_in(db, 'order:first', {'amount': 10})
            with closing(self.store.connect()) as reader:
                self.assertEqual(0, reader.execute('SELECT count(*) FROM orders').fetchone()[0])
                self.assertEqual(0, reader.execute('SELECT count(*) FROM events').fetchone()[0])
        with self.store.transaction() as db:
            self.assertEqual(1, db.execute('SELECT count(*) FROM orders').fetchone()[0])
        self.assertEqual({'amount': 10}, self.store.claim('worker', 0)['payload'])

    def test_conflict_rolls_back_business_change(self):
        self.store.enqueue('existing', {'amount': 1})
        with self.assertRaises(ValueError):
            with self.store.transaction() as db:
                db.execute('INSERT INTO orders VALUES(?,?)', ('first', 10))
                self.store.enqueue_in(db, 'existing', {'amount': 2})
        with self.store.transaction() as db:
            self.assertEqual(0, db.execute('SELECT count(*) FROM orders').fetchone()[0])

    def test_requires_explicit_transaction(self):
        with closing(sqlite3.connect(self.store.path)) as db:
            with self.assertRaises(ValueError):
                self.store.enqueue_in(db, 'a', {})

    def test_rejects_invalid_lease_times(self):
        for value in [float('nan'), float('inf'), -float('inf'), True, '1']:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.store.claim('worker', value)
                with self.assertRaises(ValueError):
                    self.store.claim('worker', 0, value)
        with self.assertRaises(ValueError):
            self.store.claim('worker', 1e308, 1e308)

    def test_connection_is_closed_after_transaction(self):
        with self.store.transaction() as db:
            db.execute('SELECT 1')
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute('SELECT 1')

    def test_failed_transaction_closes_connection(self):
        with self.assertRaises(RuntimeError):
            with self.store.transaction() as db:
                raise RuntimeError('fail')
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute('SELECT 1')
