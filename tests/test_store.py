import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from parcel_outbox.store import LeaseLost, Outbox


class StoreTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Outbox(Path(directory.name) / 'events.db')

    def test_idempotent_enqueue_rejects_changed_payload(self):
        first = self.store.enqueue('a', {'total': 1})
        self.assertEqual(first, self.store.enqueue('a', {'total': 1}))
        with self.assertRaises(ValueError):
            self.store.enqueue('a', {'total': 2})

    def test_concurrent_claims_are_exclusive(self):
        self.store.enqueue('a', {})
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda n: self.store.claim(str(n), 0), range(8)))
        self.assertEqual(1, sum(row is not None for row in results))

    def test_generation_fences_stale_worker_with_same_owner(self):
        self.store.enqueue('a', {})
        old = self.store.claim('worker', 0, 5)
        new = self.store.claim('worker', 5, 5)
        with self.assertRaises(LeaseLost):
            self.store.acknowledge(old['id'], 'worker', old['generation'], 6)
        self.store.acknowledge(new['id'], 'worker', new['generation'], 6)
        self.assertIsNone(self.store.claim('other', 100))

    def test_ack_at_expiry_fails(self):
        self.store.enqueue('a', {})
        row = self.store.claim('w', 0, 5)
        with self.assertRaises(LeaseLost):
            self.store.acknowledge(row['id'], 'w', row['generation'], 5)

    def test_lease_persists_across_reopen(self):
        self.store.enqueue('a', {'nested': [1, 2]})
        row = self.store.claim('w', 0, 10)
        reopened = Outbox(self.store.path)
        self.assertIsNone(reopened.claim('next', 9))
        self.assertEqual(row['id'], reopened.claim('next', 10)['id'])

    def test_empty_queue(self):
        self.assertIsNone(self.store.claim('w', 0))
