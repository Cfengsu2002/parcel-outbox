import tempfile
import threading
import unittest
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from parcel_outbox.deadline import DeadlineDelivery
from parcel_outbox.store import Outbox
from parcel_outbox.transport import DeliveryResult, HttpDelivery
from parcel_outbox.worker import Worker


class WorkerTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'events.db'
        self.store = Outbox(self.path)
        self.event_id = self.store.enqueue('order', {'amount': 12})
        self.now = 0

    def worker(self, send, **options):
        class Transport:
            pass
        transport = Transport()
        transport.send = send
        return Worker(self.store, transport, 'worker', clock=lambda: self.now,
                      backoff=lambda attempt: 10, **options)

    def row(self):
        with closing(self.store.connect()) as db:
            return dict(db.execute('SELECT * FROM events WHERE id=?', (self.event_id,)).fetchone())

    def test_retry_survives_worker_restart_then_success_is_acknowledged(self):
        worker = self.worker(lambda *args: DeliveryResult(503, True))
        self.assertEqual('retry', worker.run_once())
        self.now = 9
        self.assertEqual('idle', worker.run_once())
        self.store = Outbox(self.path)
        self.now = 10
        worker = self.worker(lambda *args: DeliveryResult(204, False))
        self.assertEqual('delivered', worker.run_once())
        self.assertEqual(2, self.row()['attempts'])
        self.assertEqual(1, self.row()['delivered'])

    def test_terminal_response_and_bad_body_are_dead_lettered(self):
        self.assertEqual('dead', self.worker(lambda *args: DeliveryResult(400, False)).run_once())
        self.assertEqual(1, self.row()['dead'])
        self.store.replay(self.event_id, 0)
        def invalid(*args):
            raise ValueError('body too large')
        self.assertEqual('dead', self.worker(invalid).run_once())
        self.assertEqual(1, self.row()['dead'])

    def test_crashed_claims_exhaust_budget_without_another_send(self):
        first = self.store.claim('crashed', 0, 1, max_attempts=2)
        second = self.store.claim('crashed', 1, 1, max_attempts=2)
        self.assertEqual(2, second['attempts'])
        self.assertIsNone(self.store.claim('other', 1.5, max_attempts=2))
        self.assertEqual(0, self.row()['dead'])
        self.now = 2
        def unexpected(*args):
            self.fail('exhausted event must not be sent')
        self.assertEqual('idle', self.worker(unexpected, max_attempts=2).run_once())
        self.assertEqual(1, self.row()['dead'])
        self.store.replay(self.event_id, 2)
        current = self.store.claim('new', 2, max_attempts=2)
        self.assertEqual(1, current['attempts'])
        self.assertGreater(current['generation'], first['generation'])

    def test_expired_sender_cannot_acknowledge_after_reclaim(self):
        def reclaim(*args):
            self.now = 31
            self.store.claim('replacement', self.now)
            return DeliveryResult(200, False)
        self.assertEqual('lease_lost', self.worker(reclaim).run_once())
        self.assertEqual(0, self.row()['delivered'])
        self.assertEqual('replacement', self.row()['owner'])

    def test_shutdown_finishes_inflight_delivery_and_leaves_next_event_pending(self):
        self.store.enqueue('next', {})
        def send(*args):
            worker.stop()
            return DeliveryResult(200, False)
        worker = self.worker(send)
        self.assertEqual('delivered', worker.run_once())
        self.assertEqual('stopped', worker.run_once())
        self.assertEqual('next', self.store.claim('another', 0)['event_key'])

    def test_transient_failures_dead_letter_at_budget(self):
        worker = self.worker(lambda *args: DeliveryResult(None, True), max_attempts=2)
        self.assertEqual('retry', worker.run_once())
        self.now = 10
        self.assertEqual('dead', worker.run_once())
        self.assertEqual(1, self.row()['dead'])
        self.now = 100
        self.assertEqual('idle', worker.run_once())

    def test_real_http_retry_then_success_commits_delivery(self):
        received = []

        class Receiver(BaseHTTPRequestHandler):
            def do_POST(self):
                received.append(self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(503 if len(received) == 1 else 200)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            transport = HttpDelivery(f'http://127.0.0.1:{server.server_port}/hook',
                                     b'k' * 32, allow_loopback_http=True)
            worker = Worker(self.store, DeadlineDelivery(transport, 3), 'http-worker', clock=lambda: self.now,
                            backoff=lambda attempt: 10)
            self.assertEqual('retry', worker.run_once())
            self.now = 10
            self.assertEqual('delivered', worker.run_once())
            self.assertEqual(received[0], received[1])
            self.assertEqual(1, self.row()['delivered'])
            self.assertEqual(2, self.row()['attempts'])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
