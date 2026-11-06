import hashlib
import hmac
import json
import math
import re
import sqlite3
from contextlib import closing


class InvalidDelivery(ValueError):
    pass


def _reject_constant(value):
    raise InvalidDelivery('nonfinite JSON number')


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidDelivery('duplicate JSON field')
        result[key] = value
    return result


class Receiver:
    """校验原始请求并在业务数据库中原子记录收件去重。"""
    def __init__(self, path, secrets, tolerance=300, max_body_bytes=1024 * 1024):
        secrets = tuple(secrets)
        if not 1 <= len(secrets) <= 8 or any(not isinstance(s, bytes) or len(s) < 32 for s in secrets):
            raise ValueError('one to eight signing keys of at least 32 bytes required')
        if type(tolerance) is not int or tolerance < 0:
            raise ValueError('nonnegative integer tolerance required')
        if type(max_body_bytes) is not int or max_body_bytes < 1:
            raise ValueError('positive body limit required')
        self.path = str(path)
        self.secrets = secrets
        self.tolerance = tolerance
        self.max_body_bytes = max_body_bytes
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS parcel_inbox (
                event_key TEXT PRIMARY KEY, payload TEXT NOT NULL, received_at REAL NOT NULL)''')

    def verify(self, body, timestamp, signature, now):
        if not isinstance(body, bytes) or len(body) > self.max_body_bytes:
            raise InvalidDelivery('invalid body size')
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
            raise ValueError('finite current time required')
        if not isinstance(timestamp, str) or re.fullmatch(r'0|[1-9][0-9]{0,15}', timestamp) is None:
            raise InvalidDelivery('invalid timestamp')
        if abs(now - int(timestamp)) > self.tolerance:
            raise InvalidDelivery('timestamp outside acceptance window')
        if not isinstance(signature, str) or re.fullmatch(r'v1=[0-9a-f]{64}', signature) is None:
            raise InvalidDelivery('invalid signature format')
        signed = timestamp.encode('ascii') + b'.' + body
        matched = False
        # 遍历所有轮换密钥，不因首个匹配提前返回。
        for secret in self.secrets:
            expected = 'v1=' + hmac.new(secret, signed, hashlib.sha256).hexdigest()
            matched |= hmac.compare_digest(signature, expected)
        if not matched:
            raise InvalidDelivery('signature mismatch')
        try:
            event = json.loads(body.decode('utf-8'), parse_constant=_reject_constant,
                               object_pairs_hook=_unique_object)
            if (not isinstance(event, dict) or set(event) != {'event_key', 'payload'}
                    or not isinstance(event['event_key'], str) or not event['event_key'].strip()):
                raise InvalidDelivery('invalid event envelope')
            # JSON 中的超大指数也可能解析为无穷，必须再次拒绝。
            json.dumps(event, allow_nan=False)
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise InvalidDelivery('invalid JSON envelope') from exc
        return event

    def receive(self, body, timestamp, signature, now, apply):
        event = self.verify(body, timestamp, signature, now)
        encoded = json.dumps(event['payload'], sort_keys=True, separators=(',', ':'), allow_nan=False)
        with closing(sqlite3.connect(self.path, timeout=10)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT payload FROM parcel_inbox WHERE event_key=?',
                             (event['event_key'],)).fetchone()
            if old is not None:
                if old[0] != encoded:
                    raise InvalidDelivery('event key already has different payload')
                return False
            db.execute('INSERT INTO parcel_inbox VALUES(?,?,?)',
                       (event['event_key'], encoded, now))
            # 回调必须只在此连接内写业务数据，不提交事务或执行外部副作用。
            apply(db, event['event_key'], event['payload'])
            if not db.in_transaction:
                raise RuntimeError('receiver callback must not commit the transaction')
        return True
