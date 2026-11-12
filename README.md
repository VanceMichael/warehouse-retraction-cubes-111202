# retraction-cubes

版本化播放事件（DWD）到三种粒度汇总（DWS）的可撤回计算链路。事件支持初次、改写、删除同一谱系，撤回会同步重算日频道、日作者、周作者三张表，不再手工补负数。

## 数据组织

DWD（只追加，永不覆盖）：

- `play_events`：`(event_id, revision)` 为主键，记录每次 upsert/delete 与内容哈希、来源批次；删除是带完整谱系的墓碑。
- `play_current`：每个 `event_id` 的当前生效版本与存活状态，删除保留 `deleted` 墓碑，阻止旧版复活。
- `event_audit`：同版不同内容（`conflict`）与旧版覆盖新版（`stale`）的拒绝留痕。

贡献台账（可追溯的中间层）：

- `contributions`：每条**存活**事件在三个粒度各占一行，携带分组键、viewer、时长、来源 `event_id`、`revision` 与发布 `version`。撤回即删台账行、按新维度重插；删除事件不留贡献。

DWS（三个粒度，共享发布版本）：

- `dws_daily_channel(day, channel)`、`dws_daily_author(day, author)`、`dws_weekly_author(week, author)`，周键为 ISO-8601（如 `2026-W40`）。
- 每行含 `plays / viewers / total_seconds / version`；`avg_seconds` 只在查询时由 `total_seconds / plays` 以十进制导出，不存储、不相加。
- `publish_log`、`publish_entries`：发布版本历史与每个事件的变动明细（含新旧分组键）。

## 计算规则

- **谱系**：`revision` 必须递增；同版同内容重放不贡献；同版不同内容记冲突不改数；旧版不能覆盖新版。
- **撤回传播**：一次修订先定位旧版本在三粒度的旧分组，再加入新版本的新分组，只对受影响组重算，不全扫事件。
- **去重用户数**：受影响组从台账 `COUNT(DISTINCT viewer)` 重新聚合，绝不做“减一”。
- **平均时长**：导出量，按正确分母即时计算，绝不参与加减。
- **空组**：最后一条贡献撤回后三个粒度统一表现为“无行”，不存在残留零值行；恢复贡献后组重新出现。
- **原子发布**：整批在单个 `BEGIN IMMEDIATE` 事务内完成，三张表的变动组共享同一 `version`，运营连续读取看到的是同一发布版本；任一步失败整体回滚，重试靠事件主键与批次摘要幂等，不会重复扣减。
- 输入批次以内容 SHA-256 固定；同批次重投不重复写入，同批次内容不同报错。关系数据在 SQLite（WAL），导出对象按内容哈希命名、读取重新校验。

结构定义在 `warehouse/definition.py`，业务计算在 `warehouse/domain.py`，事务装载在 `warehouse/catalog.py`，内容寻址 JSONL 对象在 `warehouse/store.py`。

## 输入与样例

输入包形如 `{"events": [...]}`，upsert 携带 `day/author/channel/viewer/seconds`，delete 只携带 `event_id/revision/op`。`examples/` 下三份合成数据（无真实用户）构成完整场景：

- `source.json`：5 条初次播放（含同一用户同组多次播放）。
- `events-02-revise.json`：跨日/跨频道/跨周改写、viewer 与时长更正、删除最后一条贡献。
- `events-03-restore.json`：删除后恢复、同版重放、同版不同内容冲突、旧版回插。
- `expected.json`：三批发布后的完整三表快照（含每行版本与平均时长）。

## 环境与验证

Python 3.11 以上，仅用标准库。`runtime.txt` 记录验证解释器版本。

```bash
python3 -m compileall -q warehouse tests scripts
python3 -m unittest discover -s tests -v
```

27 项测试覆盖：跨周移动、同一用户多次播放、撤回再恢复、同版冲突、旧版拒绝、distinct 重算、均值导出、空组一致性、提交前故障重试不重复扣减、断电重开数据库、贡献谱系与导出往返。

## 运行

```bash
python3 scripts/demo.py
```

演示使用全新临时目录，逐批真实导入，并把**独立从原始 JSON 回放谱系的全量重算结果**与数据库增量结果逐行逐字段比对（而非只打印完成），随后执行内置 `verify` 交叉校验、冲突与谱系查询、导出往返、断电注入与重试。

分步调用：

```bash
python3 -m warehouse import  --db /tmp/w.sqlite3 --input examples/source.json --batch b1
python3 -m warehouse import  --db /tmp/w.sqlite3 --input examples/events-02-revise.json --batch b2
python3 -m warehouse inspect --db /tmp/w.sqlite3          # 三表统一快照与发布版本
python3 -m warehouse verify  --db /tmp/w.sqlite3          # 增量结果 vs 明细全量重算
python3 -m warehouse conflicts --db /tmp/w.sqlite3        # 冲突/过期审计
python3 -m warehouse lineage --db /tmp/w.sqlite3 --event e4
python3 -m warehouse publishes --db /tmp/w.sqlite3       # 发布版本历史
python3 -m warehouse export  --db /tmp/w.sqlite3 --objects /tmp/objects
```
