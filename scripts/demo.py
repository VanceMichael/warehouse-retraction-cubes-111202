"""端到端小数据演示。

每一步都打印真实命令与输出；关键环节不是打印“完成”，而是：
1. 从三份原始事件 JSON 独立重算三个粒度（不经过 warehouse 的增量代码）；
2. 与数据库增量汇总逐行逐字段比对；
3. 再用内置 verify 交叉校验；
4. 演示同版冲突、删除-恢复谱系、发布历史、导出往返与断电重试。
"""
import json
import subprocess
import sys
import tempfile
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from warehouse.catalog import connect
from warehouse.domain import diff_against_reference, publish_bundle
from warehouse.store import ObjectStore

BATCHES = (
    ('examples/source.json', 'b1-initial'),
    ('examples/events-02-revise.json', 'b2-revise'),
    ('examples/events-03-restore.json', 'b3-restore'),
)
GRAINS = (('daily_channel', ('day', 'channel')),
          ('daily_author', ('day', 'author')),
          ('weekly_author', ('week', 'author')))


def execute(*arguments):
    command = [sys.executable, '-m', 'warehouse', *arguments]
    print('$ ' + ' '.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=True)
    print(result.stdout.strip(), flush=True)
    return json.loads(result.stdout)


def iso_week(day_text):
    iso = date.fromisoformat(day_text).isocalendar()
    return f'{iso[0]:04d}-W{iso[1]:02d}'


def independent_recompute():
    """完全独立地从原始事件文件回放谱系，得到三个粒度的期望值。"""
    current = {}
    for relative, _ in BATCHES:
        for event in json.loads((ROOT / relative).read_text(encoding='utf-8'))['events']:
            state = current.get(event['event_id'])
            if state is not None and event['revision'] <= state['revision']:
                continue  # 与库内规则一致：旧版不覆盖，同版不重复
            current[event['event_id']] = event
    buckets = {grain: {} for grain, _ in GRAINS}
    for event in current.values():
        if event['op'] == 'delete':
            continue
        keys = ((event['day'], event['channel']),
                (event['day'], event['author']),
                (iso_week(event['day']), event['author']))
        for (grain, _), key in zip(GRAINS, keys):
            bucket = buckets[grain].setdefault(
                key, {'plays': 0, 'viewers': set(), 'total_seconds': 0})
            bucket['plays'] += 1
            bucket['viewers'].add(event['viewer'])
            bucket['total_seconds'] += event['seconds']
    expected = {}
    for grain, (dim1, dim2) in GRAINS:
        rows = []
        for (value1, value2), bucket in sorted(buckets[grain].items()):
            avg = (Decimal(bucket['total_seconds']) / Decimal(bucket['plays'])).quantize(
                Decimal('0.000001'), rounding=ROUND_HALF_UP)
            rows.append({dim1: value1, dim2: value2, 'plays': bucket['plays'],
                         'viewers': len(bucket['viewers']),
                         'total_seconds': bucket['total_seconds'],
                         'avg_seconds': str(avg)})
        expected[grain] = rows
    return expected


def compare_row_by_row(snapshot, expected):
    print('\n== 逐项比对：独立明细重算 vs 数据库增量结果 ==', flush=True)
    checked = 0
    for grain, _ in GRAINS:
        actual = [{key: row[key] for key in row if key != 'version'}
                  for row in snapshot[grain]]
        wanted = expected[grain]
        assert len(actual) == len(wanted), f'{grain} 行数不一致'
        for index, (left, right) in enumerate(zip(actual, wanted)):
            assert left == right, f'{grain} 第 {index} 行不一致: {left} != {right}'
            checked += 1
            print(f'  [{grain}] 行{index}: {json.dumps(left, ensure_ascii=False, sort_keys=True)} ✓',
                  flush=True)
    print(f'共逐行比对 {checked} 个分组，全部一致（distinct 与均值均按集合/除法语义重算）', flush=True)


def main():
    with tempfile.TemporaryDirectory() as temporary:
        database = str(Path(temporary) / 'warehouse.sqlite3')
        objects = str(Path(temporary) / 'objects')

        for relative, batch in BATCHES:
            outcome = execute('import', '--db', database,
                              '--input', str(ROOT / relative), '--batch', batch)
            assert outcome['changed'] is True or outcome['replays'] > 0

        # 同批次重放必须无副作用
        repeated = execute('import', '--db', database,
                           '--input', str(ROOT / BATCHES[0][0]), '--batch', BATCHES[0][1])
        assert repeated['changed'] is False, '重复批次产生了额外写入'

        snapshot_view = execute('inspect', '--db', database)
        compare_row_by_row(snapshot_view, independent_recompute())

        verify = execute('verify', '--db', database)
        assert verify['differences'] == [], '内置明细重算校验发现差异'

        audit = execute('conflicts', '--db', database)
        assert len(audit) == 1 and audit[0]['reason'] == 'conflict'
        execute('lineage', '--db', database, '--event', 'e4')
        publishes = execute('publishes', '--db', database)
        assert [row['version'] for row in publishes] == [1, 2, 3]

        exported = execute('export', '--db', database, '--objects', objects)
        restored = ObjectStore(objects).read(exported['object'])
        flat = []
        for grain, _ in GRAINS:
            flat.extend({'grain': grain, **row} for row in snapshot_view[grain])
        assert restored == flat, '导出对象与查询结果不符'
        print('对象存储内容哈希往返校验通过', flush=True)

        # 断电模拟：提交前抛错，事务回滚后重试，不能重复扣减
        crash_db = str(Path(temporary) / 'crash.sqlite3')
        connection = connect(crash_db)
        first = json.loads((ROOT / BATCHES[0][0]).read_text(encoding='utf-8'))
        second = json.loads((ROOT / BATCHES[1][0]).read_text(encoding='utf-8'))
        publish_bundle(connection, first, BATCHES[0][1])

        def panic(connection, progress):
            raise RuntimeError('模拟保存进度时断电')

        try:
            publish_bundle(connection, second, BATCHES[1][1], before_commit=panic)
        except RuntimeError as error:
            print(f'\n断电注入：{error}，事务已回滚', flush=True)
        before_retry = connection.execute('SELECT COUNT(*) FROM contributions').fetchone()[0]
        publish_bundle(connection, second, BATCHES[1][1])
        after_retry = connection.execute('SELECT COUNT(*) FROM contributions').fetchone()[0]
        assert diff_against_reference(connection) == []
        assert after_retry == 12, f'重试后台账行数异常: {after_retry}'
        print(f'断电重试成功：回滚后台账 {before_retry} 行，重试后 {after_retry} 行，无重复扣减',
              flush=True)

        # 重开数据库（模拟服务重启），状态完整可继续发布
        connection.close()
        reopened = connect(crash_db)
        assert diff_against_reference(reopened) == []
        reopened.close()
        print('数据库重开后重算校验通过，演示全部成功', flush=True)


if __name__ == '__main__':
    main()
