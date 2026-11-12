import argparse
import json
from pathlib import Path

from .catalog import connect
from .domain import (
    conflicts,
    diff_against_reference,
    lineage,
    publish_bundle,
    publish_history,
    snapshot,
)
from .store import ObjectStore


def _load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def main():
    parser = argparse.ArgumentParser(description='版本化播放事件装载与可撤回汇总')
    parser.add_argument('command', choices=['import', 'inspect', 'export', 'verify', 'lineage', 'conflicts', 'publishes'])
    parser.add_argument('--db', required=True)
    parser.add_argument('--input')
    parser.add_argument('--batch')
    parser.add_argument('--objects')
    parser.add_argument('--event')
    args = parser.parse_args()
    if args.command == 'import' and (not args.input or not args.batch):
        parser.error('导入需要 --input 和 --batch')
    if args.command == 'export' and not args.objects:
        parser.error('导出需要 --objects')
    if args.command == 'lineage' and not args.event:
        parser.error('谱系查询需要 --event')
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(args.db)
    try:
        if args.command == 'import':
            result = publish_bundle(connection, _load(args.input), args.batch)
        elif args.command == 'inspect':
            result = snapshot(connection)
        elif args.command == 'verify':
            result = {'publish_version': snapshot(connection)['publish_version'],
                      'differences': diff_against_reference(connection)}
        elif args.command == 'lineage':
            result = lineage(connection, args.event)
        elif args.command == 'conflicts':
            result = conflicts(connection)
        else:
            result = publish_history(connection)
        if args.command == 'export':
            view = snapshot(connection)
            records = []
            for grain, rows in ((key, view[key]) for key in ('daily_channel', 'daily_author', 'weekly_author')):
                for row in rows:
                    records.append({'grain': grain, **row})
            result = {'publish_version': view['publish_version'],
                      'object': ObjectStore(args.objects).put(records), 'rows': len(records)}
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
