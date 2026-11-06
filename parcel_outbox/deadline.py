import math
import multiprocessing
import time

from parcel_outbox.transport import DeliveryResult


def _send_in_child(sender, pipe, event_key, payload, timestamp):
    try:
        result = sender.send(event_key, payload, timestamp)
        pipe.send(('result', result.status, result.retryable))
    except ValueError:
        pipe.send(('invalid',))
    except Exception:
        # 只传递分类结果，不跨进程返回可能含凭据的异常正文。
        pipe.send(('failure',))
    finally:
        pipe.close()


class DeadlineDelivery:
    """用独立进程隔离 DNS 和阻塞 I/O，超时终止本地发送进程。"""
    def __init__(self, sender, deadline_seconds=10):
        if (isinstance(deadline_seconds, bool) or not isinstance(deadline_seconds, (int, float))
                or not math.isfinite(deadline_seconds) or deadline_seconds <= 0):
            raise ValueError('positive finite delivery deadline required')
        self.sender = sender
        self.deadline_seconds = deadline_seconds

    def send(self, event_key, payload, timestamp):
        context = multiprocessing.get_context('spawn')
        reader, writer = context.Pipe(duplex=False)
        process = context.Process(target=_send_in_child,
                                  args=(self.sender, writer, event_key, payload, timestamp))
        end = time.monotonic() + self.deadline_seconds
        started = False
        try:
            process.start()
            started = True
            writer.close()
            remaining = max(0, end - time.monotonic())
            if not reader.poll(remaining):
                return DeliveryResult(None, True)
            try:
                message = reader.recv()
            except (EOFError, OSError):
                return DeliveryResult(None, True)
            if time.monotonic() > end:
                return DeliveryResult(None, True)
            if message[0] == 'invalid':
                raise ValueError('delivery input rejected')
            if message[0] == 'result':
                return DeliveryResult(message[1], message[2])
            return DeliveryResult(None, True)
        finally:
            reader.close()
            writer.close()
            if started:
                process.join(timeout=0)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=.2)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=.2)
                if not process.is_alive():
                    process.close()
