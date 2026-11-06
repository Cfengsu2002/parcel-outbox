import math
import threading
import time

from parcel_outbox.store import LeaseLost
from parcel_outbox.transport import retry_delay


class Worker:
    def __init__(self, store, transport, owner, lease_seconds=30, max_attempts=5,
                 clock=time.time, backoff=retry_delay):
        if not isinstance(owner, str) or not owner.strip():
            raise ValueError('nonempty owner required')
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError('positive integer attempt limit required')
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, (int, float)) or not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError('positive finite lease required')
        deadline = getattr(transport, 'deadline_seconds', None)
        if deadline is not None and deadline + 1 >= lease_seconds:
            raise ValueError('lease must exceed delivery deadline by more than one second')
        self.store = store
        self.transport = transport
        self.owner = owner
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.clock = clock
        self.backoff = backoff
        self.stopping = threading.Event()

    def stop(self):
        # 停止领取新任务；已发送的请求仍完成状态确认，不伪装成撤回网络请求。
        self.stopping.set()

    def run_once(self):
        if self.stopping.is_set():
            return 'stopped'
        event = self.store.claim(self.owner, self.clock(), self.lease_seconds,
                                 max_attempts=self.max_attempts)
        if event is None:
            return 'idle'
        try:
            try:
                result = self.transport.send(event['event_key'], event['payload'], int(self.clock()))
            except ValueError:
                # 无法序列化或超出请求大小限制的事件不可通过重试修复。
                self.store.fail(event['id'], self.owner, event['generation'], self.clock(), max_attempts=1)
                return 'dead'
            if result.delivered:
                self.store.acknowledge(event['id'], self.owner, event['generation'], self.clock())
                return 'delivered'
            retryable = result.retryable
            self.store.fail(event['id'], self.owner, event['generation'], self.clock(),
                            delay=self.backoff(event['attempts']) if retryable else 0,
                            max_attempts=self.max_attempts if retryable else 1)
            return 'dead' if not retryable or event['attempts'] >= self.max_attempts else 'retry'
        except LeaseLost:
            # 网络发送成功不代表仍持有租约，过期消费者不能修改队列状态。
            return 'lease_lost'

    def run(self, poll_seconds=1):
        if isinstance(poll_seconds, bool) or not isinstance(poll_seconds, (int, float)) or not math.isfinite(poll_seconds) or poll_seconds <= 0:
            raise ValueError('positive finite poll interval required')
        while not self.stopping.is_set():
            if self.run_once() == 'idle':
                self.stopping.wait(poll_seconds)
