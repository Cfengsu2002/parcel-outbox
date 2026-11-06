import multiprocessing
import os
import tempfile
import time
import unittest
from pathlib import Path

from parcel_outbox.deadline import DeadlineDelivery
from parcel_outbox.store import Outbox
from parcel_outbox.transport import DeliveryResult
from parcel_outbox.worker import Worker


class SlowSender:
    def send(self, *args):
        time.sleep(10)
        return DeliveryResult(200, False)


class SuccessfulSender:
    def send(self, *args):
        return DeliveryResult(204, False)


class InvalidSender:
    def send(self, *args):
        raise ValueError('private detail must not propagate')


class CrashedSender:
    def send(self, *args):
        os._exit(2)


class DeadlineTests(unittest.TestCase):
    def test_blocked_sender_is_terminated_and_no_child_remains(self):
        before = {p.pid for p in multiprocessing.active_children()}
        start = time.monotonic()
        result = DeadlineDelivery(SlowSender(), .5).send('key', {}, 100)
        self.assertIsNone(result.status)
        self.assertTrue(result.retryable)
        self.assertLess(time.monotonic() - start, 3)
        self.assertEqual(before, {p.pid for p in multiprocessing.active_children()})

    def test_success_and_input_failure_survive_spawn_boundary(self):
        result = DeadlineDelivery(SuccessfulSender(), 3).send('key', {}, 100)
        self.assertEqual(204, result.status)
        self.assertTrue(result.delivered)
        with self.assertRaisesRegex(ValueError, '^delivery input rejected$'):
            DeadlineDelivery(InvalidSender(), 3).send('key', {}, 100)

    def test_crash_before_reply_is_retryable(self):
        result = DeadlineDelivery(CrashedSender(), 3).send('key', {}, 100)
        self.assertIsNone(result.status)
        self.assertTrue(result.retryable)

    def test_invalid_budget_and_inadequate_lease_rejected(self):
        for value in (0, -1, True, float('inf'), float('nan')):
            with self.assertRaises(ValueError):
                DeadlineDelivery(SuccessfulSender(), value)
        transport = DeadlineDelivery(SuccessfulSender(), 3)
        with self.assertRaises(ValueError):
            Worker(None, transport, 'worker', lease_seconds=4)

    def test_worker_persists_retry_after_transport_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Outbox(Path(directory) / 'events.db')
            store.enqueue('key', {})
            now = time.time()
            worker = Worker(store, DeadlineDelivery(SlowSender(), .5), 'worker',
                            lease_seconds=5, clock=time.time, backoff=lambda attempt: 10)
            self.assertEqual('retry', worker.run_once())
            self.assertIsNone(store.claim('other', now + 9))
            retry = store.claim('other', time.time() + 11)
            self.assertEqual(2, retry['attempts'])
