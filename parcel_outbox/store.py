import json
import math
from contextlib import closing, contextmanager
import sqlite3


class LeaseLost(RuntimeError):
    pass


class Outbox:
    def __init__(self, path):
        self.path = str(path)
        with closing(self.connect()) as db, db:
            db.executescript('''PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY, event_key TEXT UNIQUE NOT NULL,
                    payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
                    owner TEXT, lease_until REAL, generation INTEGER NOT NULL DEFAULT 0);
            ''')
        with self.transaction() as db:
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version > 1:
                raise RuntimeError('database schema is newer than this application')
            if version == 0:
                for column in ('available_at REAL NOT NULL DEFAULT 0',
                               'attempts INTEGER NOT NULL DEFAULT 0',
                               'dead INTEGER NOT NULL DEFAULT 0'):
                    db.execute('ALTER TABLE events ADD COLUMN ' + column)
                db.execute('CREATE INDEX ready_events ON events(delivered, dead, available_at)')
                db.execute('PRAGMA user_version=1')

    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    @contextmanager
    def transaction(self):
        """Share one commit boundary between application writes and events."""
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            yield db

    def enqueue(self, key, payload):
        with self.transaction() as db:
            return self.enqueue_in(db, key, payload)

    @staticmethod
    def enqueue_in(db, key, payload):
        """Insert into the caller's transaction without committing it."""
        if not db.in_transaction:
            raise ValueError('an active transaction is required')
        if not isinstance(key, str) or not key.strip():
            raise ValueError('a nonempty event key is required')
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
        old = db.execute('SELECT id,payload FROM events WHERE event_key=?', (key,)).fetchone()
        if old:
            if old[1] != encoded:
                raise ValueError('event key already has different payload')
            return old[0]
        return db.execute('INSERT INTO events(event_key,payload) VALUES(?,?)',
                          (key, encoded)).lastrowid

    @staticmethod
    def _validate_time(value):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError('time must be finite')

    def claim(self, owner, now, seconds=30, max_attempts=5):
        self._validate_time(now)
        self._validate_time(seconds)
        self._validate_time(now + seconds)
        if not isinstance(owner, str) or not owner.strip() or seconds <= 0:
            raise ValueError('owner and positive lease required')
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError('positive integer attempt limit required')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            # 已耗尽预算的过期租约进入死信，仍有效的租约不得被其他消费者终止。
            db.execute('''UPDATE events SET dead=1, owner=NULL, lease_until=NULL
                WHERE delivered=0 AND dead=0 AND attempts>=?
                AND (lease_until IS NULL OR lease_until<=?)''', (max_attempts, now))
            row = db.execute('''SELECT * FROM events WHERE delivered=0 AND dead=0 AND available_at<=?
                AND (lease_until IS NULL OR lease_until<=?) ORDER BY id LIMIT 1''', (now, now)).fetchone()
            if row is None:
                return None
            db.execute('''UPDATE events SET owner=?, lease_until=?, generation=generation+1, attempts=attempts+1
                WHERE id=?''', (owner, now+seconds, row['id']))
            result = dict(db.execute('SELECT * FROM events WHERE id=?', (row['id'],)).fetchone())
            result['payload'] = json.loads(result['payload'])
            return result

    def acknowledge(self, event_id, owner, generation, now):
        self._validate_time(now)
        with closing(self.connect()) as db, db:
            changed = db.execute('''UPDATE events SET delivered=1,owner=NULL,lease_until=NULL
                WHERE id=? AND delivered=0 AND owner=? AND generation=? AND lease_until>?''',
                                 (event_id, owner, generation, now)).rowcount
            if changed != 1:
                raise LeaseLost('lease expired or replaced')

    def fail(self, event_id, owner, generation, now, delay=0, max_attempts=5):
        """Release a current lease or dead-letter its exhausted delivery."""
        self._validate_time(now)
        self._validate_time(delay)
        self._validate_time(now + delay)
        if delay < 0 or type(max_attempts) is not int or max_attempts < 1:
            raise ValueError('nonnegative delay and positive integer attempt limit required')
        with self.transaction() as db:
            changed = db.execute('''UPDATE events SET owner=NULL, lease_until=NULL,
                available_at=?, dead=CASE WHEN attempts>=? THEN 1 ELSE 0 END
                WHERE id=? AND delivered=0 AND dead=0 AND owner=?
                AND generation=? AND lease_until>?''',
                (now+delay, max_attempts, event_id, owner, generation, now)).rowcount
            if changed != 1:
                raise LeaseLost('lease expired or replaced')

    def replay(self, event_id, now):
        """Explicitly reset a dead event while preserving its fencing generation."""
        self._validate_time(now)
        with self.transaction() as db:
            changed = db.execute('''UPDATE events SET dead=0, attempts=0, available_at=?
                WHERE id=? AND dead=1 AND delivered=0''', (now, event_id)).rowcount
            if changed != 1:
                raise ValueError('event is not dead-lettered')
