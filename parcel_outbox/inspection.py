import math
import sqlite3
import time
from contextlib import closing
from pathlib import Path


STATES = ('delivered', 'dead', 'leased', 'waiting', 'ready')


def inspect_events(path, after=0, limit=100, state=None, through=None, now=None):
    """按事件 ID 分页读取元数据；不返回用户键、载荷或租约所有者。"""
    if after is None:
        raise ValueError('nonnegative integer cursor required')
    for value in (after, through):
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError('nonnegative integer cursor required')
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError('limit must be between one and 500')
    if state is not None and state not in STATES:
        raise ValueError('unknown event state')
    now = time.time() if now is None else now
    if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
        raise ValueError('finite current time required')
    uri = Path(path).resolve().as_uri() + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True)) as db, db:
        db.row_factory = sqlite3.Row
        # 本页水位和数据使用同一读事务，避免首个分页水位与本页内容错位。
        db.execute('BEGIN')
        if through is None:
            through = db.execute('SELECT coalesce(max(id),0) FROM events').fetchone()[0]
        records = db.execute('''WITH metadata AS (
            SELECT id, attempts, generation, available_at, lease_until,
                CASE WHEN delivered=1 THEN 'delivered' WHEN dead=1 THEN 'dead'
                     WHEN lease_until>? THEN 'leased' WHEN available_at>? THEN 'waiting'
                     ELSE 'ready' END AS state
            FROM events WHERE id>? AND id<=?)
            SELECT * FROM metadata WHERE (? IS NULL OR state=?) ORDER BY id LIMIT ?''',
            (now, now, after, through, state, state, limit + 1)).fetchall()
        more = len(records) > limit
        events = [dict(row) for row in records[:limit]]
        return {'events': events, 'through': through,
                'next_after': events[-1]['id'] if more else None}
