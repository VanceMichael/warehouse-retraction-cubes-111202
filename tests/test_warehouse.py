import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from warehouse.catalog import connect, load_bundle, transaction
from warehouse.domain import (
    conflicts,
    diff_against_reference,
    iso_week,
    lineage,
    publish_bundle,
    publish_history,
    reference_recompute,
    snapshot,
    validate_bundle,
)
from warehouse.store import ObjectStore

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / 'examples'
SOURCE = EXAMPLES / 'source.json'
REVISE = EXAMPLES / 'events-02-revise.json'
RESTORE = EXAMPLES / 'events-03-restore.json'
EXPECTED = json.loads((EXAMPLES / 'expected.json').read_text(encoding='utf-8'))


def event(event_id, revision, op='upsert', **fields):
    record = {'event_id': event_id, 'revision': revision, 'op': op}
    if op == 'upsert':
        record.update(fields)
    return record


def play(event_id, revision, *, day='2026-10-01', author='a1', channel='c1',
         viewer='u1', seconds=10):
    return event(event_id, revision, day=day, author=author, channel=channel,
                 viewer=viewer, seconds=seconds)


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / 'warehouse.sqlite3'
        self.connection = connect(self.path)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def load(self, path, batch):
        return publish_bundle(self.connection, json.loads(path.read_text(encoding='utf-8')), batch)

    def load_events(self, events, batch):
        return publish_bundle(self.connection, {'events': events}, batch)

    def assert_matches_reference(self):
        self.assertEqual(diff_against_reference(self.connection), [])


class CatalogTests(PipelineTest):
    def test_replay_is_idempotent(self):
        first = self.load(SOURCE, 'b1')
        self.assertTrue(first['changed'])
        view = snapshot(self.connection)
        again = self.load(SOURCE, 'b1')
        self.assertFalse(again['changed'])
        self.assertEqual(snapshot(self.connection), view)
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 1)
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM play_events').fetchone()[0], 5)
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM contributions').fetchone()[0], 15)

    def test_conflicting_batch_digest_rejected(self):
        self.load(SOURCE, 'b1')
        with self.assertRaises(ValueError):
            publish_bundle(self.connection, {'events': []}, 'b1')
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 1)

    def test_duplicate_key_rolls_back_in_generic_loader(self):
        bundle = {'play_events': [{
            'event_id': 'x', 'revision': 0, 'op': 'upsert', 'day': '2026-10-01',
            'author': 'a', 'channel': 'c', 'viewer': 'u', 'seconds': 1,
            'content_hash': 'h', 'batch': 't'}]}
        with self.assertRaises(sqlite3.IntegrityError):
            load_bundle(self.connection, copy.deepcopy(bundle) | {
                'play_events': bundle['play_events'] * 2}, 'broken')
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM play_events').fetchone()[0], 0)

    def test_unknown_collection_rejected(self):
        with self.assertRaises(ValueError):
            publish_bundle(self.connection, {'plays': []}, 'x')
        with self.assertRaises(ValueError):
            publish_bundle(self.connection, {}, 'x')

    def test_transaction_failure_leaves_nothing(self):
        with self.assertRaises(RuntimeError):
            with transaction(self.connection):
                self.connection.execute(
                    'INSERT INTO ingest_batches VALUES (?, ?, ?)', ('partial', 'abc', 1))
                raise RuntimeError('模拟写入中断')
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM ingest_batches').fetchone()[0], 0)

    def test_invalid_records(self):
        good = play('e', 0)
        valid_delete = event('e', 0, op='delete')
        for bad in (
            event('', 0),
            {**play('e', 0), 'revision': -1},
            {**play('e', 0), 'op': 'erase'},
            {'event_id': 'e', 'revision': 0, 'op': 'delete', 'day': '2026-10-01'},
            play('e', 0, seconds=-1),
            {**play('e', 0), 'seconds': 1.5},
            play('e', 0, day='not-a-date'),
        ):
            with self.assertRaises(ValueError):
                validate_bundle({'events': [bad]}, 'b')
        validate_bundle({'events': [good]}, 'b')
        validate_bundle({'events': [valid_delete]}, 'b')

    def test_empty_batch_records_progress_without_publish(self):
        result = self.load_events([], 'empty')
        self.assertFalse(result['changed'])
        self.assertEqual(self.connection.execute(
            'SELECT rows FROM ingest_batches').fetchone()[0], 0)
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM publish_log').fetchone()[0], 0)

    def test_persistence_across_reopen(self):
        self.load(SOURCE, 'b1')
        self.load(REVISE, 'b2')
        self.connection.close()
        self.connection = connect(self.path)
        self.assertFalse(self.load(SOURCE, 'b1')['changed'])
        self.assert_matches_reference()


class RetractionTests(PipelineTest):
    def test_three_batches_match_expected_snapshot(self):
        self.load(SOURCE, 'b1')
        self.load(REVISE, 'b2')
        result = self.load(RESTORE, 'b3')
        self.assertEqual(result['replays'], 2)
        self.assertEqual(result['conflicts'], 1)
        self.assertEqual(snapshot(self.connection), EXPECTED)
        self.assert_matches_reference()

    def test_every_batch_agrees_with_full_recompute(self):
        for path, batch in ((SOURCE, 'b1'), (REVISE, 'b2'), (RESTORE, 'b3')):
            self.load(path, batch)
            self.assert_matches_reference()
            # 参照重算只依赖 DWD 当前态，与增量 DWS 行数一致
            view = snapshot(self.connection)
            reference = reference_recompute(self.connection)
            for grain in ('daily_channel', 'daily_author', 'weekly_author'):
                self.assertEqual(len(view[grain]), len(reference[grain]))

    def test_distinct_is_recomputed_not_decremented(self):
        events = [
            play('e1', 0, viewer='u1', seconds=10),
            play('e2', 0, viewer='u1', seconds=20),
            play('e3', 0, viewer='u2', seconds=30),
        ]
        self.load_events(events, 'b1')
        row = self.connection.execute(
            'SELECT plays, viewers FROM dws_daily_channel').fetchone()
        self.assertEqual((row['plays'], row['viewers']), (3, 2))
        # 删掉 u2 的唯一一条：distinct 必须由台账重算得到 1，而不是沿用或盲目减
        self.load_events([event('e3', 1, op='delete')], 'b2')
        row = self.connection.execute(
            'SELECT plays, viewers FROM dws_daily_channel').fetchone()
        self.assertEqual((row['plays'], row['viewers']), (2, 1))
        # 再删 u1 的其中一条：distinct 保持 1，只是次数下降
        self.load_events([event('e2', 1, op='delete')], 'b3')
        row = self.connection.execute(
            'SELECT plays, viewers FROM dws_daily_channel').fetchone()
        self.assertEqual((row['plays'], row['viewers']), (1, 1))
        self.assert_matches_reference()

    def test_average_is_derived_not_summed(self):
        self.load_events([
            play('e1', 0, seconds=10),
            play('e2', 0, seconds=20),
        ], 'b1')
        self.assertEqual(snapshot(self.connection)['daily_channel'][0]['avg_seconds'], '15.000000')
        # 改写时长后均值必须由 总时长/次数 重新导出
        self.load_events([play('e2', 1, seconds=50)], 'b2')
        row = snapshot(self.connection)['daily_channel'][0]
        self.assertEqual(row['total_seconds'], 60)
        self.assertEqual(row['avg_seconds'], '30.000000')
        self.assert_matches_reference()

    def test_empty_group_removed_consistently_and_can_return(self):
        self.load(SOURCE, 'b1')
        # e4 是 (2026-10-01, c2) / (2026-10-01, a2) / (W40, a2) 的唯一贡献
        self.load_events([event('e4', 1, op='delete')], 'b2')
        # 唯一贡献被撤回后，三个粒度的空组都是“无行”，不存在残留零值行
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM dws_daily_channel WHERE day='2026-10-01' AND channel='c2'"
        ).fetchone()[0], 0)
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM dws_daily_author WHERE day='2026-10-01' AND author='a2'"
        ).fetchone()[0], 0)
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM dws_weekly_author WHERE week='2026-W40' AND author='a2'"
        ).fetchone()[0], 0)
        # 空组在三表中都表现为“无行”，与从未存在过的组查询结果一致
        ghost = self.connection.execute(
            "SELECT COUNT(*) FROM dws_daily_channel WHERE day='2099-01-01'").fetchone()[0]
        self.assertEqual(ghost, 0)
        # 撤回后恢复：空组重新出现
        self.load_events([play('e4', 2, day='2026-10-01', author='a2',
                               channel='c2', viewer='u3', seconds=50)], 'b3')
        row = self.connection.execute(
            "SELECT plays, viewers, total_seconds FROM dws_daily_channel "
            "WHERE day='2026-10-01' AND channel='c2'").fetchone()
        self.assertEqual(tuple(row), (1, 1, 50))
        self.assert_matches_reference()

    def test_cross_week_move_rebalances_both_weeks(self):
        self.assertEqual(iso_week('2026-10-04'), '2026-W40')  # 周日仍属 W40
        self.assertEqual(iso_week('2026-10-05'), '2026-W41')  # 周一进入 W41
        self.load(SOURCE, 'b1')
        self.load(REVISE, 'b2')
        weeks = {row['week']: row for row in snapshot(self.connection)['weekly_author']}
        # e2 从 W40 移到 W41 且换频道；W40 的 a1 剩 e1(15) 与改 viewer 的 e3(25)
        self.assertEqual((weeks['2026-W40']['plays'], weeks['2026-W40']['viewers'],
                          weeks['2026-W40']['total_seconds']), (2, 1, 40))
        self.assertEqual((weeks['2026-W41']['plays'], weeks['2026-W41']['viewers'],
                          weeks['2026-W41']['total_seconds']), (2, 1, 70))
        self.assert_matches_reference()

    def test_same_viewer_multiple_plays_counts_once(self):
        self.load(SOURCE, 'b1')
        row = self.connection.execute(
            "SELECT plays, viewers FROM dws_daily_channel "
            "WHERE day='2026-10-01' AND channel='c1'").fetchone()
        self.assertEqual((row['plays'], row['viewers']), (3, 2))

    def test_retract_then_restore_full_cycle(self):
        self.load(SOURCE, 'b1')
        self.load_events([event('e4', 1, op='delete')], 'b2')
        self.assertEqual(self.connection.execute(
            "SELECT status FROM play_current WHERE event_id='e4'").fetchone()[0], 'deleted')
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM contributions WHERE event_id='e4'").fetchone()[0], 0)
        self.load_events([event('e4', 2, day='2026-10-01', author='a2',
                                channel='c2', viewer='u3', seconds=50)], 'b3')
        record = lineage(self.connection, 'e4')
        self.assertEqual([v['revision'] for v in record['versions']], [0, 1, 2])
        self.assertEqual([v['op'] for v in record['versions']], ['upsert', 'delete', 'upsert'])
        self.assertEqual(record['current']['status'], 'live')
        self.assertEqual(len(record['contributions']), 3)
        self.assert_matches_reference()

    def test_same_revision_different_content_is_conflict(self):
        self.load(SOURCE, 'b1')
        tampered = event('e5', 0, day='2026-10-05', author='a1',
                         channel='c1', viewer='u1', seconds=99)
        result = self.load_events([tampered], 'b2')
        self.assertEqual(result['conflicts'], 1)
        audit = conflicts(self.connection)
        self.assertEqual(len(audit), 1)
        self.assertEqual((audit[0]['event_id'], audit[0]['revision'], audit[0]['reason']),
                         ('e5', 0, 'conflict'))
        # 被篡改内容没有生效
        self.assertEqual(self.connection.execute(
            "SELECT seconds FROM play_current WHERE event_id='e5'").fetchone()[0], 40)
        self.assert_matches_reference()

    def test_stale_revision_in_gap_cannot_overwrite(self):
        self.load_events([play('e1', 2)], 'b1')
        result = self.load_events([play('e1', 1)], 'b2')
        self.assertEqual(result['stale'], 1)
        audit = conflicts(self.connection)
        self.assertEqual(audit[0]['reason'], 'stale')
        self.assertEqual(self.connection.execute(
            'SELECT revision FROM play_current WHERE event_id=?', ('e1',)).fetchone()[0], 2)
        # 同版同内容重放不贡献、不记冲突
        again = self.load_events([play('e1', 2)], 'b3')
        self.assertEqual((again['replays'], again['changes'], again['conflicts']), (1, 0, 0))
        self.assert_matches_reference()

    def test_atomic_version_shared_across_three_tables(self):
        self.load(SOURCE, 'b1')
        self.load(REVISE, 'b2')
        view = snapshot(self.connection)
        self.assertEqual(view['publish_version'], 2)
        touched_versions = set()
        for grain in ('daily_channel', 'daily_author', 'weekly_author'):
            for row in view[grain]:
                touched_versions.add(row['version'])
        # 本次发布触及的组都带版本 2；未触及的组保留版本 1，证明只重算受影响组
        self.assertEqual(touched_versions, {1, 2})
        history = publish_history(self.connection)
        self.assertEqual([row['version'] for row in history], [1, 2])
        self.assertEqual([row['changes'] for row in history], [5, 4])

    def test_failure_before_commit_then_retry_no_double_deduction(self):
        self.load(SOURCE, 'b1')

        def blow_up(connection, progress):
            raise RuntimeError('模拟保存进度时断电')

        with self.assertRaises(RuntimeError):
            publish_bundle(self.connection,
                           json.loads(REVISE.read_text(encoding='utf-8')), 'b2',
                           before_commit=blow_up)
        # 事务整体回滚：没有发布记录、没有批次进度、台账未被部分扣减
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM publish_log').fetchone()[0], 1)
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM ingest_batches WHERE batch=?', ('b2',)).fetchone()[0], 0)
        self.assertEqual(self.connection.execute(
            'SELECT revision FROM play_current WHERE event_id=?', ('e1',)).fetchone()[0], 0)
        before = snapshot(self.connection)
        result = self.load(REVISE, 'b2')
        self.assertTrue(result['changed'])
        # 重试恰好生效一次，无重复扣减
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM publish_log').fetchone()[0], 2)
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM contributions').fetchone()[0],
            4 * 3)  # e4 已删除，四条存活事件各三粒度
        self.assert_matches_reference()
        self.assertNotEqual(before, snapshot(self.connection))

    def test_power_loss_reopen_and_resume(self):
        self.load(SOURCE, 'b1')
        self.load(REVISE, 'b2')
        self.connection.close()
        wal = self.path.with_name(self.path.name + '-wal')
        self.connection = connect(self.path)
        # 重开后已发布版本不丢，可继续追加新发布
        self.assertEqual(snapshot(self.connection)['publish_version'], 2)
        self.load(RESTORE, 'b3')
        self.assertEqual(snapshot(self.connection), EXPECTED)
        self.assert_matches_reference()
        self.connection.close()
        self.connection = connect(self.path)
        self.assertFalse(self.load(SOURCE, 'b1')['changed'])
        # WAL 已随正常关闭 checkpoint，库文件自洽可读
        self.assertTrue(self.path.exists())
        self.assertFalse(wal.exists() and wal.stat().st_size > 0)

    def test_lineage_explains_every_contribution(self):
        self.load(SOURCE, 'b1')
        self.load(REVISE, 'b2')
        record = lineage(self.connection, 'e2')
        self.assertEqual(len(record['versions']), 2)
        self.assertEqual(record['current']['channel'], 'c2')
        grains = {row['grain']: row for row in record['contributions']}
        self.assertEqual(set(grains), {'daily_channel', 'daily_author', 'weekly_author'})
        self.assertEqual(grains['weekly_author']['dim1'], '2026-W41')
        self.assertEqual([entry['version'] for entry in record['publishes']], [1, 2])
        detail = json.loads(record['publishes'][1]['detail'])
        self.assertEqual(detail['old_groups'][0], ['daily_channel', '2026-10-01', 'c1'])
        self.assertEqual(detail['new_groups'][0], ['daily_channel', '2026-10-05', 'c2'])
        # 台账行数 = 存活事件数 × 3，删除事件不留贡献
        live = self.connection.execute(
            "SELECT COUNT(*) FROM play_current WHERE status='live'").fetchone()[0]
        self.assertEqual(self.connection.execute(
            'SELECT COUNT(*) FROM contributions').fetchone()[0], live * 3)

    def test_export_roundtrip_through_object_store(self):
        self.load(SOURCE, 'b1')
        self.load(REVISE, 'b2')
        self.load(RESTORE, 'b3')
        view = snapshot(self.connection)
        records = []
        for grain in ('daily_channel', 'daily_author', 'weekly_author'):
            records.extend({'grain': grain, **row} for row in view[grain])
        store = ObjectStore(Path(self.temporary.name) / 'objects')
        digest = store.put(records)
        self.assertEqual(store.read(digest), records)


class ObjectTests(unittest.TestCase):
    def test_roundtrip_and_reuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ObjectStore(temporary)
            rows = [{'标识': '合成数据', 'value': None}, {'number': 3}]
            digest = store.put(rows)
            self.assertEqual(store.read(digest), rows)
            self.assertEqual(store.put(rows), digest)
            self.assertEqual(len(list(Path(temporary).glob('*.jsonl'))), 1)

    def test_corrupt_object(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ObjectStore(temporary)
            digest = store.put([{'value': 1}])
            (Path(temporary) / (digest + '.jsonl')).write_bytes(b'{}')
            with self.assertRaises(ValueError):
                store.read(digest)

    def test_empty_and_invalid_identifier(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ObjectStore(temporary)
            self.assertEqual(store.read(store.put([])), [])
            with self.assertRaises(ValueError):
                store.read('../outside')

    def test_nonfinite_number(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                ObjectStore(temporary).put([{'value': float('nan')}])


if __name__ == '__main__':
    unittest.main()
