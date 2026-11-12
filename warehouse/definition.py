# DWD 明细与 DWS 汇总的全部关系结构。连接建立时幂等创建。
SCHEMA = {
    # DWD：同一 event_id 的初次、改写、删除按 revision 追加，永不覆盖
    'play_events': (
        'CREATE TABLE IF NOT EXISTS play_events ('
        'event_id TEXT NOT NULL, '
        'revision INTEGER NOT NULL, '
        'op TEXT NOT NULL, '
        'day TEXT, '
        'author TEXT, '
        'channel TEXT, '
        'viewer TEXT, '
        'seconds INTEGER, '
        'content_hash TEXT NOT NULL, '
        'batch TEXT NOT NULL, '
        'PRIMARY KEY (event_id, revision))'
    ),
    # DWD：每个谱系当前生效的版本；删除保留墓碑，阻止旧版复活
    'play_current': (
        'CREATE TABLE IF NOT EXISTS play_current ('
        'event_id TEXT PRIMARY KEY, '
        'revision INTEGER NOT NULL, '
        'status TEXT NOT NULL, '
        'day TEXT, '
        'author TEXT, '
        'channel TEXT, '
        'viewer TEXT, '
        'seconds INTEGER)'
    ),
    # 同版不同内容、旧版覆盖新版等被拒绝消息的审计留痕
    'event_audit': (
        'CREATE TABLE IF NOT EXISTS event_audit ('
        'id INTEGER PRIMARY KEY AUTOINCREMENT, '
        'event_id TEXT NOT NULL, '
        'revision INTEGER NOT NULL, '
        'reason TEXT NOT NULL, '
        'content_hash TEXT NOT NULL, '
        'batch TEXT NOT NULL, '
        'detail TEXT NOT NULL, '
        'created_at TEXT NOT NULL)'
    ),
    # DWS 贡献台账：每条存活事件在三个粒度各留一行，携带来源与发布版本
    'contributions': (
        'CREATE TABLE IF NOT EXISTS contributions ('
        'event_id TEXT NOT NULL, '
        'grain TEXT NOT NULL, '
        'dim1 TEXT NOT NULL, '
        'dim2 TEXT NOT NULL, '
        'viewer TEXT NOT NULL, '
        'seconds INTEGER NOT NULL, '
        'revision INTEGER NOT NULL, '
        'version INTEGER NOT NULL, '
        'PRIMARY KEY (event_id, grain))'
    ),
    'dws_daily_channel': (
        'CREATE TABLE IF NOT EXISTS dws_daily_channel ('
        'day TEXT NOT NULL, '
        'channel TEXT NOT NULL, '
        'plays INTEGER NOT NULL, '
        'viewers INTEGER NOT NULL, '
        'total_seconds INTEGER NOT NULL, '
        'version INTEGER NOT NULL, '
        'PRIMARY KEY (day, channel))'
    ),
    'dws_daily_author': (
        'CREATE TABLE IF NOT EXISTS dws_daily_author ('
        'day TEXT NOT NULL, '
        'author TEXT NOT NULL, '
        'plays INTEGER NOT NULL, '
        'viewers INTEGER NOT NULL, '
        'total_seconds INTEGER NOT NULL, '
        'version INTEGER NOT NULL, '
        'PRIMARY KEY (day, author))'
    ),
    'dws_weekly_author': (
        'CREATE TABLE IF NOT EXISTS dws_weekly_author ('
        'week TEXT NOT NULL, '
        'author TEXT NOT NULL, '
        'plays INTEGER NOT NULL, '
        'viewers INTEGER NOT NULL, '
        'total_seconds INTEGER NOT NULL, '
        'version INTEGER NOT NULL, '
        'PRIMARY KEY (week, author))'
    ),
    # 一次事实修订对应一个发布版本，三张汇总表在同一事务内共同推进
    'publish_log': (
        'CREATE TABLE IF NOT EXISTS publish_log ('
        'version INTEGER PRIMARY KEY, '
        'batch TEXT NOT NULL, '
        'created_at TEXT NOT NULL, '
        'changes INTEGER NOT NULL, '
        'replays INTEGER NOT NULL, '
        'conflicts INTEGER NOT NULL, '
        'stale INTEGER NOT NULL)'
    ),
    'publish_entries': (
        'CREATE TABLE IF NOT EXISTS publish_entries ('
        'version INTEGER NOT NULL, '
        'event_id TEXT NOT NULL, '
        'action TEXT NOT NULL, '
        'old_revision INTEGER, '
        'new_revision INTEGER NOT NULL, '
        'detail TEXT NOT NULL, '
        'PRIMARY KEY (version, event_id))'
    ),
}

INDEXES = {
    'idx_contributions_group': 'CREATE INDEX IF NOT EXISTS idx_contributions_group ON contributions (grain, dim1, dim2)',
    'idx_play_current_status': 'CREATE INDEX IF NOT EXISTS idx_play_current_status ON play_current (status)',
    'idx_event_audit_event': 'CREATE INDEX IF NOT EXISTS idx_event_audit_event ON event_audit (event_id, revision)',
}

# 粒度 -> 汇总表 / 维度命名 / 第一维度是否按周归并
GRAINS = (
    ('daily_channel', 'dws_daily_channel', ('day', 'channel'), False),
    ('daily_author', 'dws_daily_author', ('day', 'author'), False),
    ('weekly_author', 'dws_weekly_author', ('week', 'author'), True),
)

OPS = ('upsert', 'delete')
AUDIT_CONFLICT = 'conflict'
AUDIT_STALE = 'stale'
STATUS_LIVE = 'live'
STATUS_DELETED = 'deleted'
# 平均时长只在查询时由 总时长/次数 导出，保留六位小数（四舍五入）
AVG_QUANTUM = '0.000001'
