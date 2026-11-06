import hashlib
import hmac
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from parcel_outbox.transport import HttpDelivery, retry_delay


class TransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.requests = []

        class Receiver(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                cls.requests.append((self.path, dict(self.headers), body))
                if self.path == '/disconnect':
                    self.close_connection = True
                    return
                status = int(self.path.split('?')[0][1:])
                self.send_response(status)
                if status == 302:
                    self.send_header('Location', '/200')
                self.end_headers()

            def log_message(self, *args):
                pass

        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def endpoint(self, path):
        return f'http://127.0.0.1:{self.server.server_port}{path}'

    def delivery(self, path, **kwargs):
        return HttpDelivery(self.endpoint(path), b'k' * 32,
                            allow_loopback_http=True, **kwargs)

    def test_receiver_verifies_exact_body_and_timestamp_signature(self):
        result = self.delivery('/200?source=outbox').send('订单\n1', {'z': 1, 'a': '你好'}, 123)
        self.assertTrue(result.delivered)
        path, headers, body = self.requests[-1]
        self.assertEqual('/200?source=outbox', path)
        expected = hmac.new(b'k' * 32, b'123.' + body, hashlib.sha256).hexdigest()
        self.assertEqual('v1=' + expected, headers['Parcel-Signature'])
        self.assertEqual('123', headers['Parcel-Timestamp'])
        self.assertEqual('订单\n1', json.loads(body)['event_key'])
        self.assertNotIn('\n', headers['Idempotency-Key'])
        self.delivery('/200').send('订单\n1', {'new': 2}, 124)
        self.assertEqual(headers['Idempotency-Key'], self.requests[-1][1]['Idempotency-Key'])

    def test_status_classification_and_redirect_not_followed(self):
        for status, retryable in [(204, False), (302, False), (400, False),
                                  (408, True), (425, True), (429, True), (503, True)]:
            with self.subTest(status=status):
                before = len(self.requests)
                result = self.delivery('/' + str(status)).send('key', {}, 1)
                self.assertEqual(status, result.status)
                self.assertEqual(retryable, result.retryable)
                self.assertEqual(200 <= status < 300, result.delivered)
                self.assertEqual(before + 1, len(self.requests))

    def test_connection_closed_before_response_is_retryable(self):
        result = self.delivery('/disconnect').send('key', {}, 1)
        self.assertIsNone(result.status)
        self.assertTrue(result.retryable)
        self.assertFalse(result.delivered)

    def test_endpoint_validation_does_not_silently_downgrade_tls(self):
        for endpoint in ['http://example.com/', 'http://localhost/', 'https://user:pass@example.com/',
                         'https://example.com/#fragment', 'https://example.com/\nInjected', 'file:///tmp/a']:
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(ValueError):
                    HttpDelivery(endpoint, b'k' * 32, allow_loopback_http=True)
        with self.assertRaises(ValueError):
            HttpDelivery(self.endpoint('/200'), b'k' * 32)

    def test_validation_happens_before_any_request(self):
        before = len(self.requests)
        for kwargs in [{'secret': b'short'}, {'secret': b'k' * 32, 'timeout': float('nan')},
                       {'secret': b'k' * 32, 'max_body_bytes': True}]:
            with self.assertRaises(ValueError):
                HttpDelivery('https://example.com/', **kwargs)
        with self.assertRaises(ValueError):
            self.delivery('/200', max_body_bytes=8).send('key', {}, 1)
        with self.assertRaises(ValueError):
            self.delivery('/200').send('key', {'bad': float('nan')}, 1)
        with self.assertRaises(ValueError):
            self.delivery('/200').send('key', {}, True)
        self.assertEqual(before, len(self.requests))

    def test_exponential_jitter_is_bounded_for_large_attempts(self):
        self.assertEqual(.5, retry_delay(1, sample=lambda: .5))
        self.assertEqual(2, retry_delay(3, sample=lambda: .5))
        self.assertEqual(150, retry_delay(1000000, sample=lambda: .5))
        self.assertEqual(0, retry_delay(4, sample=lambda: 0))
        for attempt, opts in [(True, {}), (0, {}), (1, {'base': float('inf')}),
                              (1, {'sample': lambda: 1.1})]:
            with self.assertRaises(ValueError):
                retry_delay(attempt, **opts)
