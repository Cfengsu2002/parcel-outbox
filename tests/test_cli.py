import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from parcel_outbox.store import Outbox


class CliTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'events.db'
        self.command = [sys.executable, '-m', 'parcel_outbox', '--db', str(self.path)]
        self.env = dict(os.environ, PARCEL_SIGNING_KEY_HEX='6b' * 32)

    def run_cli(self, *args, payload=None, env=None):
        return subprocess.run(self.command + list(args), input=payload, text=True,
                              capture_output=True, env=env or self.env, timeout=10)

    def test_enqueue_idempotency_status_and_safe_conflict_error(self):
        first = self.run_cli('enqueue', '--key', 'private-key', payload='{"private":1}')
        second = self.run_cli('enqueue', '--key', 'private-key', payload='{"private":1}')
        self.assertEqual(0, first.returncode)
        self.assertEqual(first.stdout, second.stdout)
        conflict = self.run_cli('enqueue', '--key', 'private-key', payload='{"private":2}')
        self.assertEqual(1, conflict.returncode)
        self.assertNotIn('private', conflict.stderr + conflict.stdout)
        result = self.run_cli('status')
        self.assertEqual({'total': 1, 'pending': 1, 'dead': 0, 'delivered': 0}, json.loads(result.stdout))
        self.assertNotIn('private', result.stdout)

    def test_status_does_not_create_missing_database(self):
        result = self.run_cli('status')
        self.assertEqual(1, result.returncode)
        self.assertFalse(self.path.exists())

    def test_bad_worker_config_rejected_before_database_creation_without_secret_leak(self):
        result = self.run_cli('work', '--endpoint', 'http://user:password@example.com', '--once')
        self.assertEqual(1, result.returncode)
        self.assertFalse(self.path.exists())
        self.assertNotIn('password', result.stderr)
        self.assertNotIn('6b6b6b', result.stderr)
        result = self.run_cli('work', '--endpoint', 'https://example.com', '--deadline', '10', '--lease', '10', '--once')
        self.assertEqual(1, result.returncode)
        self.assertFalse(self.path.exists())

    def test_invalid_or_oversized_json_does_not_enqueue(self):
        for payload in ('not json', '{"x":NaN}', ' ' * (1024 * 1024 + 1)):
            self.assertEqual(1, self.run_cli('enqueue', '--key', 'a', payload=payload).returncode)
        if self.path.exists():
            self.assertEqual(0, json.loads(self.run_cli('status').stdout)['total'])

    def test_spawned_cli_worker_delivers_real_http_then_status_reports_success(self):
        received = []
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                received.append(self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(204)
                self.end_headers()
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            self.run_cli('enqueue', '--key', 'a', payload='{"amount":1}')
            result = self.run_cli('work', '--endpoint', f'http://127.0.0.1:{server.server_port}/hook',
                                  '--allow-loopback-http', '--once')
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual({'state': 'delivered'}, json.loads(result.stdout))
            self.assertEqual('a', json.loads(received[0])['event_key'])
            self.assertEqual(1, json.loads(self.run_cli('status').stdout)['delivered'])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_sigterm_stops_idle_worker_cleanly(self):
        process = subprocess.Popen(self.command + ['work', '--endpoint', 'https://example.com'],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=self.env)
        try:
            self.assertEqual({'state': 'started'}, json.loads(process.stdout.readline()))
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(0, process.returncode, stderr)
            self.assertEqual({'state': 'stopped'}, json.loads(stdout))
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            process.stdout.close()
            process.stderr.close()

    def test_replay_resets_only_dead_event(self):
        store = Outbox(self.path)
        event_id = store.enqueue('a', {})
        event = store.claim('worker', 0)
        store.fail(event_id, 'worker', event['generation'], 1, max_attempts=1)
        result = self.run_cli('replay', str(event_id))
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual({'replayed': event_id}, json.loads(result.stdout))
        self.assertEqual(1, json.loads(self.run_cli('status').stdout)['pending'])
        self.assertEqual(1, self.run_cli('replay', str(event_id)).returncode)

    def test_inspect_cli_pages_metadata_and_replay_missing_db_does_not_create_it(self):
        missing = self.run_cli('replay', '1')
        self.assertEqual(1, missing.returncode)
        self.assertFalse(self.path.exists())
        for i in range(3):
            self.run_cli('enqueue', '--key', f'private-{i}', payload='{"secret":1}')
        result = self.run_cli('inspect', '--limit', '2', '--state', 'ready')
        self.assertEqual(0, result.returncode, result.stderr)
        first = json.loads(result.stdout)
        self.assertEqual(2, first['next_after'])
        self.assertNotIn('private', result.stdout)
        self.assertNotIn('secret', result.stdout)
        second = self.run_cli('inspect', '--after', str(first['next_after']),
                              '--through', str(first['through']))
        self.assertEqual([3], [x['id'] for x in json.loads(second.stdout)['events']])
