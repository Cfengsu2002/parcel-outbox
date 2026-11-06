import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from parcel_outbox.store import LeaseLost, Outbox


class RetryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'events.db'
        self.store = Outbox(self.path)
        self.event_id = self.store.enqueue('a', {'amount': 1})

    def test_backoff_survives_reopen_and_is_ready_at_boundary(self):
        event = self.store.claim('w', 0, 30)
        self.store.fail(event['id'], 'w', event['generation'], 1, delay=10)
        reopened = Outbox(self.path)
        self.assertIsNone(reopened.claim('w', 10.9))
        retried = reopened.claim('w', 11)
        self.assertEqual(2, retried['attempts'])
        self.assertGreater(retried['generation'], event['generation'])

    def test_dead_letter_requires_explicit_replay(self):
        first = self.store.claim('w', 0)
        self.store.fail(self.event_id, 'w', first['generation'], 1, max_attempts=1)
        self.assertIsNone(self.store.claim('w', 100))
        self.store.replay(self.event_id, 101)
        self.assertIsNone(self.store.claim('w', 100))
        second = self.store.claim('w', 101)
        self.assertEqual(1, second['attempts'])
        self.assertGreater(second['generation'], first['generation'])
        with self.assertRaises(LeaseLost):
            self.store.fail(self.event_id, 'w', first['generation'], 102)
        self.store.acknowledge(self.event_id, 'w', second['generation'], 102)
        with self.assertRaises(ValueError):
            self.store.replay(self.event_id, 103)

    def test_expired_lease_cannot_schedule_retry(self):
        event = self.store.claim('w', 0, 1)
        with self.assertRaises(LeaseLost):
            self.store.fail(self.event_id, 'w', event['generation'], 1)
        self.assertEqual(2, self.store.claim('other', 1)['attempts'])

    def test_rejects_invalid_retry_settings_without_losing_lease(self):
        event = self.store.claim('w', 0)
        for values in [{'delay': -1}, {'delay': float('nan')}, {'max_attempts': True},
                       {'max_attempts': 0}, {'max_attempts': 1.5}]:
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    self.store.fail(self.event_id, 'w', event['generation'], 1, **values)
        self.store.acknowledge(self.event_id, 'w', event['generation'], 2)

    def test_migrates_old_database_without_losing_events(self):
        legacy = self.path.parent / 'legacy.db'
        with closing(sqlite3.connect(legacy)) as db, db:
            db.execute('''CREATE TABLE events(id INTEGER PRIMARY KEY, event_key TEXT UNIQUE NOT NULL,
                payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
                owner TEXT, lease_until REAL, generation INTEGER NOT NULL DEFAULT 0)''')
            db.execute("INSERT INTO events(event_key,payload) VALUES('legacy','{}')")
        upgraded = Outbox(legacy)
        self.assertEqual('legacy', upgraded.claim('w', 0)['event_key'])
        Outbox(legacy)
        with closing(upgraded.connect()) as db:
            self.assertEqual(1, db.execute('PRAGMA user_version').fetchone()[0])
