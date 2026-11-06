import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from parcel_outbox.inspection import inspect_events
from parcel_outbox.store import Outbox


class InspectionTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'events.db'
        self.store = Outbox(self.path)
        for i in range(5):
            self.store.enqueue(f'private-key-{i}', {'secret-value': i})

    def test_keyset_pages_exclude_events_added_after_watermark(self):
        first = inspect_events(self.path, limit=2, now=0)
        self.assertEqual([1, 2], [x['id'] for x in first['events']])
        self.assertEqual(2, first['next_after'])
        self.assertEqual(5, first['through'])
        self.store.enqueue('later', {})
        second = inspect_events(self.path, after=first['next_after'], through=first['through'], limit=2, now=0)
        third = inspect_events(self.path, after=second['next_after'], through=first['through'], limit=2, now=0)
        self.assertEqual([3, 4], [x['id'] for x in second['events']])
        self.assertEqual([5], [x['id'] for x in third['events']])
        self.assertIsNone(third['next_after'])
        self.assertEqual([6], [x['id'] for x in inspect_events(self.path, after=5, now=0)['events']])

    def test_state_filters_respect_lease_and_retry_boundaries(self):
        event = self.store.claim('private-owner', 0, 10)
        self.store.acknowledge(event['id'], 'private-owner', event['generation'], 1)
        event = self.store.claim('private-owner', 0, 10)
        self.store.fail(event['id'], 'private-owner', event['generation'], 1, max_attempts=1)
        event = self.store.claim('private-owner', 0, 10)
        self.store.fail(event['id'], 'private-owner', event['generation'], 1, delay=20)
        self.store.claim('private-owner', 0, 10)
        for state, expected in [('delivered', [1]), ('dead', [2]), ('waiting', [3]),
                                ('leased', [4]), ('ready', [5])]:
            self.assertEqual(expected, [x['id'] for x in inspect_events(self.path, state=state, now=1)['events']])
        self.assertEqual([4, 5], [x['id'] for x in inspect_events(self.path, state='ready', now=10)['events']])
        self.assertEqual([3, 4, 5], [x['id'] for x in inspect_events(self.path, state='ready', now=21)['events']])

    def test_metadata_omits_payload_keys_and_owner(self):
        self.store.claim('private-owner', 0)
        output = json.dumps(inspect_events(self.path, now=1))
        for forbidden in ('private-key', 'secret-value', 'private-owner', 'payload', 'event_key', 'owner'):
            self.assertNotIn(forbidden, output)

    def test_empty_and_missing_databases_do_not_mutate_state(self):
        empty = Path(self.path.parent) / 'empty.db'
        Outbox(empty)
        self.assertEqual({'events': [], 'through': 0, 'next_after': None}, inspect_events(empty))
        missing = Path(self.path.parent) / 'missing.db'
        with self.assertRaises(sqlite3.OperationalError):
            inspect_events(missing)
        self.assertFalse(missing.exists())
        with closing(self.store.connect()) as db:
            before = db.execute('SELECT * FROM events ORDER BY id').fetchall()
        inspect_events(self.path, now=999)
        with closing(self.store.connect()) as db:
            self.assertEqual([tuple(x) for x in before], [tuple(x) for x in db.execute('SELECT * FROM events ORDER BY id')])

    def test_invalid_parameters_rejected(self):
        for options in [{'after': None}, {'after': -1}, {'after': True}, {'through': 1.5}, {'limit': 0},
                        {'limit': 501}, {'limit': True}, {'state': "ready' OR 1=1"}, {'now': float('nan')}]:
            with self.subTest(options=options):
                with self.assertRaises(ValueError):
                    inspect_events(self.path, **options)
