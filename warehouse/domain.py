"""DWD -> DWS 可撤回计算。

设计要点：
- play_events 只追加、不覆盖：revision 递增构成同一 event_id 的谱系，删除以墓碑保留。
- contributions 是可追溯的贡献台账：每条存活事件在三个粒度各占一行，带来源 event_id、
  revision 与发布版本。撤回即删台账行，再按新维度重插，汇总只对受影响组重算。
- distinct 永远从台账重新 COUNT(DISTINCT)，绝不做“减一”；平均时长只在查询时由
  总时长/次数导出，绝不参与加减。
- 一次批次在单个事务内推进三张汇总表，所有变动组共享同一个发布版本。
"""
import hashlib
import json
import sqlite3
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

from .catalog import encode, transaction
from .definition import (
    AUDIT_CONFLICT,
    AUDIT_STALE,
    AVG_QUANTUM,
    GRAINS,
    OPS,
    STATUS_DELETED,
    STATUS_LIVE,
)

UPSERT_FIELDS = ('day', 'author', 'channel', 'viewer', 'seconds')


# ---------- 基础工具 ----------

def iso_week(day_text):
    """ISO-8601 周年/周序，例如 2026-10-04（周日）落在 2026-W40。"""
    iso = date.fromisoformat(day_text).isocalendar()
    return f'{iso[0]:04d}-W{iso[1]:02d}'


def _event_hash(event):
    payload = {key: event[key] for key in ('event_id', 'revision', 'op') if key in event}
    if event['op'] == 'upsert':
        for key in UPSERT_FIELDS:
            payload[key] = event[key]
    return hashlib.sha256(encode(payload)).hexdigest()


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _is_text(value):
    return isinstance(value, str) and bool(value)


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def validate_bundle(bundle, batch):
    if not isinstance(batch, str) or not batch:
        raise ValueError('输入需要非空批次标识')
    if not isinstance(bundle, dict) or set(bundle) != {'events'}:
        raise ValueError("输入包必须且只能包含 events 记录数组")
    events = bundle['events']
    if not isinstance(events, list):
        raise ValueError('events 必须是记录数组')
    for event in events:
        if not isinstance(event, dict):
            raise ValueError('事件必须是对象')
        if not _is_text(event.get('event_id')):
            raise ValueError('event_id 必须是非空文本')
        if not _is_int(event.get('revision')) or event['revision'] < 0:
            raise ValueError('revision 必须是非负递增整数')
        op = event.get('op')
        if op not in OPS:
            raise ValueError(f'op 只能是 {OPS} 之一')
        if op == 'upsert':
            if set(event) != {'event_id', 'revision', 'op', *UPSERT_FIELDS}:
                raise ValueError('upsert 事件字段必须为 event_id/revision/op/day/author/channel/viewer/seconds')
            for key in ('day', 'author', 'channel', 'viewer'):
                if not _is_text(event.get(key)):
                    raise ValueError(f'{key} 必须是非空文本')
            date.fromisoformat(event['day'])
            if not _is_int(event.get('seconds')) or event['seconds'] < 0:
                raise ValueError('seconds 必须是非负整数')
        elif set(event) != {'event_id', 'revision', 'op'}:
            raise ValueError('delete 事件只能携带 event_id/revision/op，业务维度随谱系历史可查')


def _group_keys(event):
    """事件在三个粒度下的分组键：(grain, dim1, dim2)。"""
    return (
        ('daily_channel', event['day'], event['channel']),
        ('daily_author', event['day'], event['author']),
        ('weekly_author', iso_week(event['day']), event['author']),
    )


def _current_group_keys(connection, event_id):
    """当前生效版本（若存活）的旧分组键，用于定位撤回影响面。"""
    record = connection.execute(
        'SELECT status, day, author, channel FROM play_current WHERE event_id=?', (event_id,)
    ).fetchone()
    if record is None or record['status'] != STATUS_LIVE:
        return ()
    return _group_keys(record)


# ---------- DWD 装载与 DWS 增量发布 ----------

def publish_bundle(connection, bundle, batch, before_commit=None):
    """装载一批版本化事件并把修订传播到三张汇总表。

    整个过程在一个 BEGIN IMMEDIATE 事务内完成：任一步失败整体回滚，重试时
    play_events 主键与批次幂等保证不会重复扣减。before_commit 仅用于故障注入测试。
    返回本次发布的版本与各类事件计数。
    """
    validate_bundle(bundle, batch)
    digest = hashlib.sha256(encode(bundle)).hexdigest()
    with transaction(connection):
        previous = connection.execute(
            'SELECT digest FROM ingest_batches WHERE batch=?', (batch,)
        ).fetchone()
        if previous is not None:
            if previous['digest'] != digest:
                raise ValueError('同批次内容不一致')
            return {'changed': False, 'version': _current_version(connection)}

        version = _current_version(connection) + 1
        counters = {'changes': 0, 'replays': 0, 'conflicts': 0, 'stale': 0}
        entries = []
        affected = set()

        for event in bundle['events']:
            content_hash = _event_hash(event)
            event_id, revision, op = event['event_id'], event['revision'], event['op']

            # 重放同版：主键已存在即不重复贡献；同版不同内容落冲突审计
            existing = connection.execute(
                'SELECT content_hash FROM play_events WHERE event_id=? AND revision=?',
                (event_id, revision),
            ).fetchone()
            if existing is not None:
                if existing['content_hash'] == content_hash:
                    counters['replays'] += 1
                    continue
                counters['conflicts'] += 1
                _audit(connection, event_id, revision, AUDIT_CONFLICT, content_hash, batch,
                       {'incoming_hash': content_hash, 'stored_hash': existing['content_hash']})
                continue

            current = connection.execute(
                'SELECT revision FROM play_current WHERE event_id=?', (event_id,)
            ).fetchone()
            if current is not None and revision <= current['revision']:
                # 旧版不能覆盖新版；同版同内容已在上面按重放处理，走到这里即冲突/过期
                reason = AUDIT_CONFLICT if revision == current['revision'] else AUDIT_STALE
                counters[reason] += 1
                _audit(connection, event_id, revision, reason, content_hash, batch,
                       {'incoming_revision': revision, 'current_revision': current['revision']})
                continue

            try:
                connection.execute(
                    'INSERT INTO play_events (event_id, revision, op, day, author, channel, '
                    'viewer, seconds, content_hash, batch) VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (event_id, revision, op,
                     event.get('day'), event.get('author'), event.get('channel'),
                     event.get('viewer'), event.get('seconds'), content_hash, batch),
                )
            except sqlite3.IntegrityError:
                # 并发/同事务内撞同版键：按冲突记录，不改变任何汇总
                counters['conflicts'] += 1
                _audit(connection, event_id, revision, AUDIT_CONFLICT, content_hash, batch,
                       {'detail': '同版键已被占用'})
                continue

            old_groups = _current_group_keys(connection, event_id)
            affected.update(old_groups)

            if op == 'delete':
                connection.execute(
                    'INSERT INTO play_current (event_id, revision, status, day, author, channel, '
                    'viewer, seconds) VALUES (?,?,?,?,?,?,?,?) '
                    'ON CONFLICT(event_id) DO UPDATE SET revision=excluded.revision, '
                    'status=excluded.status, day=NULL, author=NULL, channel=NULL, viewer=NULL, seconds=NULL',
                    (event_id, revision, STATUS_DELETED, None, None, None, None, None),
                )
                connection.execute('DELETE FROM contributions WHERE event_id=?', (event_id,))
                action = 'delete'
                new_groups = ()
            else:
                connection.execute(
                    'INSERT INTO play_current (event_id, revision, status, day, author, channel, '
                    'viewer, seconds) VALUES (?,?,?,?,?,?,?,?) '
                    'ON CONFLICT(event_id) DO UPDATE SET revision=excluded.revision, '
                    'status=excluded.status, day=excluded.day, author=excluded.author, '
                    'channel=excluded.channel, viewer=excluded.viewer, seconds=excluded.seconds',
                    (event_id, revision, STATUS_LIVE, event['day'], event['author'],
                     event['channel'], event['viewer'], event['seconds']),
                )
                connection.execute('DELETE FROM contributions WHERE event_id=?', (event_id,))
                connection.executemany(
                    'INSERT INTO contributions (event_id, grain, dim1, dim2, viewer, seconds, '
                    'revision, version) VALUES (?,?,?,?,?,?,?,?)',
                    [(event_id, grain, dim1, dim2, event['viewer'], event['seconds'],
                      revision, version) for grain, dim1, dim2 in _group_keys(event)],
                )
                action = 'upsert'
                new_groups = _group_keys(event)
                affected.update(new_groups)

            old_revision = current['revision'] if current is not None else None
            entries.append((version, event_id, action, old_revision, revision,
                            json.dumps({'old_groups': list(old_groups),
                                        'new_groups': list(new_groups)}, ensure_ascii=False)))
            counters['changes'] += 1

        if counters['changes']:
            for key in sorted(affected):
                _recompute_group(connection, key, version)
            connection.execute(
                'INSERT INTO publish_log (version, batch, created_at, changes, replays, conflicts, stale) '
                'VALUES (?,?,?,?,?,?,?)',
                (version, batch, _now(), counters['changes'], counters['replays'],
                 counters['conflicts'], counters['stale']),
            )
            connection.executemany(
                'INSERT INTO publish_entries (version, event_id, action, old_revision, new_revision, detail) '
                'VALUES (?,?,?,?,?,?)',
                entries,
            )

        connection.execute(
            'INSERT INTO ingest_batches VALUES (?, ?, ?)',
            (batch, digest, len(bundle['events'])),
        )
        if before_commit is not None:
            before_commit(connection, {'version': version, **counters})

    return {'changed': bool(counters['changes']), 'version': version if counters['changes']
            else _current_version(connection), **counters}


def _audit(connection, event_id, revision, reason, content_hash, batch, detail):
    connection.execute(
        'INSERT INTO event_audit (event_id, revision, reason, content_hash, batch, detail, created_at) '
        'VALUES (?,?,?,?,?,?,?)',
        (event_id, revision, reason, content_hash, batch,
         json.dumps(detail, ensure_ascii=False, sort_keys=True), _now()),
    )


def _current_version(connection):
    row = connection.execute('SELECT MAX(version) AS v FROM publish_log').fetchone()
    return row['v'] or 0


def _recompute_group(connection, key, version):
    """只重算受影响组：distinct 与总量都从贡献台账重新聚合，空组删除，表现一致。"""
    grain, dim1, dim2 = key
    row = connection.execute(
        'SELECT COUNT(*) AS plays, COUNT(DISTINCT viewer) AS viewers, '
        'COALESCE(SUM(seconds), 0) AS total_seconds FROM contributions '
        'WHERE grain=? AND dim1=? AND dim2=?',
        (grain, dim1, dim2),
    ).fetchone()
    table = next(spec[1] for spec in GRAINS if spec[0] == grain)
    if row['plays'] == 0:
        connection.execute(
            f'DELETE FROM {table} WHERE {GRAINS_DIMS[grain][0]}=? AND {GRAINS_DIMS[grain][1]}=?',
            (dim1, dim2),
        )
        return
    dim1_name, dim2_name = GRAINS_DIMS[grain]
    connection.execute(
        f'INSERT INTO {table} ({dim1_name}, {dim2_name}, plays, viewers, total_seconds, version) '
        f'VALUES (?,?,?,?,?,?) ON CONFLICT({dim1_name}, {dim2_name}) DO UPDATE SET '
        'plays=excluded.plays, viewers=excluded.viewers, total_seconds=excluded.total_seconds, '
        'version=excluded.version',
        (dim1, dim2, row['plays'], row['viewers'], row['total_seconds'], version),
    )


GRAINS_DIMS = {spec[0]: spec[2] for spec in GRAINS}


# ---------- 查询 ----------

def _average(plays, total_seconds):
    # 平均值是导出量：总时长除以次数，四舍五入到微秒级十进制文本，绝不参与相加
    return str((Decimal(total_seconds) / Decimal(plays)).quantize(
        Decimal(AVG_QUANTUM), rounding=ROUND_HALF_UP))


def _summary_rows(connection, grain):
    _, table, (dim1_name, dim2_name), _ = next(spec for spec in GRAINS if spec[0] == grain)
    rows = []
    for record in connection.execute(
        f'SELECT {dim1_name}, {dim2_name}, plays, viewers, total_seconds, version '
        f'FROM {table} ORDER BY {dim1_name}, {dim2_name}'
    ):
        row = dict(record)
        row['avg_seconds'] = _average(row['plays'], row['total_seconds'])
        rows.append(row)
    return rows


def daily_channel(connection):
    return _summary_rows(connection, 'daily_channel')


def daily_author(connection):
    return _summary_rows(connection, 'daily_author')


def weekly_author(connection):
    return _summary_rows(connection, 'weekly_author')


def overview(connection):
    """日频道视图（精简列，兼容原有查询习惯）。"""
    return [
        {'day': row['day'], 'channel': row['channel'], 'plays': row['plays'],
         'viewers': row['viewers'], 'total_seconds': row['total_seconds']}
        for row in daily_channel(connection)
    ]


def snapshot(connection):
    """运营连续读取的统一视图：三张表共享同一发布版本。"""
    return {
        'publish_version': _current_version(connection),
        'daily_channel': daily_channel(connection),
        'daily_author': daily_author(connection),
        'weekly_author': weekly_author(connection),
    }


def conflicts(connection):
    return [dict(record) for record in connection.execute(
        'SELECT event_id, revision, reason, content_hash, batch, detail, created_at '
        'FROM event_audit ORDER BY id')]


def lineage(connection, event_id):
    """可查的贡献来源：事件完整谱系、当前态、台账贡献与发布条目。"""
    versions = [dict(record) for record in connection.execute(
        'SELECT event_id, revision, op, day, author, channel, viewer, seconds, content_hash, batch '
        'FROM play_events WHERE event_id=? ORDER BY revision', (event_id,))]
    current = connection.execute(
        'SELECT * FROM play_current WHERE event_id=?', (event_id,)).fetchone()
    ledger = [dict(record) for record in connection.execute(
        'SELECT grain, dim1, dim2, viewer, seconds, revision, version '
        'FROM contributions WHERE event_id=? ORDER BY grain', (event_id,))]
    publishes = [dict(record) for record in connection.execute(
        'SELECT version, action, old_revision, new_revision, detail '
        'FROM publish_entries WHERE event_id=? ORDER BY version', (event_id,))]
    return {'event_id': event_id, 'versions': versions,
            'current': dict(current) if current else None,
            'contributions': ledger, 'publishes': publishes}


def publish_history(connection):
    return [dict(record) for record in connection.execute(
        'SELECT version, batch, created_at, changes, replays, conflicts, stale '
        'FROM publish_log ORDER BY version')]


# ---------- 全量参照重算（回归比对专用，不读任何 DWS 表） ----------

def reference_recompute(connection):
    """从 DWD 当前存活事实全量重算三个粒度，作为增量结果的独立参照。"""
    buckets = {spec[0]: {} for spec in GRAINS}
    for record in connection.execute(
        'SELECT day, author, channel, viewer, seconds FROM play_current WHERE status=?',
        (STATUS_LIVE,),
    ):
        for grain, dim1, dim2 in _group_keys(record):
            bucket = buckets[grain].setdefault((dim1, dim2), {'plays': 0, 'viewers': set(),
                                                               'total_seconds': 0})
            bucket['plays'] += 1
            bucket['viewers'].add(record['viewer'])
            bucket['total_seconds'] += record['seconds']
    result = {}
    for grain, _, (dim1_name, dim2_name), _ in GRAINS:
        rows = []
        for (dim1, dim2), bucket in sorted(buckets[grain].items()):
            rows.append({dim1_name: dim1, dim2_name: dim2, 'plays': bucket['plays'],
                         'viewers': len(bucket['viewers']),
                         'total_seconds': bucket['total_seconds'],
                         'avg_seconds': _average(bucket['plays'], bucket['total_seconds'])})
        result[grain] = rows
    return result


def diff_against_reference(connection):
    """逐项比对增量汇总与明细全量重算，返回不一致条目（空列表即完全一致）。"""
    reference = reference_recompute(connection)
    differences = []
    for grain in reference:
        incremental = _summary_rows(connection, grain)
        clean = [{key: row[key] for key in row if key != 'version'} for row in incremental]
        if clean != reference[grain]:
            differences.append({'grain': grain, 'incremental': clean,
                                'reference': reference[grain]})
    return differences
