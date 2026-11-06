import hashlib
import hmac
import http.client
import ipaddress
import json
import math
import random
import ssl
from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class DeliveryResult:
    status: int | None
    retryable: bool

    @property
    def delivered(self):
        return self.status is not None and 200 <= self.status < 300


def retry_delay(attempt, base=1.0, cap=300.0, sample=random.random):
    """使用有上限的全抖动退避，避免失败任务同时重试。"""
    if type(attempt) is not int or attempt < 1:
        raise ValueError('positive integer attempt required')
    if any(isinstance(v, bool) or not isinstance(v, (int, float))
           or not math.isfinite(v) or v <= 0 for v in (base, cap)):
        raise ValueError('positive finite backoff bounds required')
    unit = sample()
    if not isinstance(unit, (int, float)) or not math.isfinite(unit) or not 0 <= unit <= 1:
        raise ValueError('random sample must be between zero and one')
    ceiling = min(base, cap)
    for _ in range(min(attempt - 1, 2098)):
        ceiling = min(cap, ceiling * 2)
        if ceiling == cap:
            break
    return ceiling * unit


class HttpDelivery:
    """仅发送到运维配置的可信端点，不接受任意用户提交的目标。"""
    def __init__(self, endpoint, secret, timeout=5, max_body_bytes=1024 * 1024,
                 allow_loopback_http=False):
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError('signing key must contain at least 32 bytes')
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError('positive finite timeout required')
        if type(max_body_bytes) is not int or max_body_bytes < 1:
            raise ValueError('positive body limit required')
        parsed = urlsplit(endpoint)
        if (parsed.scheme not in ('https', 'http') or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.fragment or any(ord(c) <= 32 or ord(c) == 127 for c in endpoint)):
            raise ValueError('absolute endpoint without credentials or fragment required')
        if parsed.scheme == 'http':
            try:
                loopback = ipaddress.ip_address(parsed.hostname).is_loopback
            except ValueError:
                loopback = False
            if not allow_loopback_http or not loopback:
                raise ValueError('HTTPS required except explicit loopback development mode')
        self.host = parsed.hostname
        self.port = parsed.port
        self.path = parsed.path or '/'
        if parsed.query:
            self.path += '?' + parsed.query
        self.secure = parsed.scheme == 'https'
        self.secret = secret
        self.timeout = timeout
        self.max_body_bytes = max_body_bytes

    def send(self, event_key, payload, timestamp):
        if not isinstance(event_key, str) or not event_key.strip():
            raise ValueError('nonempty event key required')
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError('nonnegative integer timestamp required')
        body = json.dumps({'event_key': event_key, 'payload': payload}, sort_keys=True,
                          separators=(',', ':'), allow_nan=False).encode('utf-8')
        if len(body) > self.max_body_bytes:
            raise ValueError('delivery body exceeds configured limit')
        stamp = str(timestamp)
        signature = hmac.new(self.secret, stamp.encode('ascii') + b'.' + body,
                             hashlib.sha256).hexdigest()
        # 使用键的哈希作为幂等标识，避免原始键中的换行进入 HTTP 头。
        dedupe = hashlib.sha256(event_key.encode('utf-8')).hexdigest()
        headers = {'Content-Type': 'application/json', 'Idempotency-Key': dedupe,
                   'Parcel-Timestamp': stamp, 'Parcel-Signature': 'v1=' + signature}
        connection = (http.client.HTTPSConnection(self.host, self.port, timeout=self.timeout,
                                                 context=ssl.create_default_context())
                      if self.secure else http.client.HTTPConnection(self.host, self.port, timeout=self.timeout))
        try:
            connection.request('POST', self.path, body=body, headers=headers)
            response = connection.getresponse()
            # 不读取响应正文，也不跟随重定向，避免无限响应和签名泄漏。
            status = response.status
            response.close()
            return DeliveryResult(status, status in (408, 425, 429) or 500 <= status < 600)
        except (OSError, http.client.HTTPException):
            return DeliveryResult(None, True)
        finally:
            connection.close()
