from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal


def overview(connection):
    rows = [dict(record) for record in connection.execute('SELECT day, channel, COUNT(*) AS plays, COUNT(DISTINCT viewer) AS viewers, SUM(seconds) AS total_seconds FROM plays GROUP BY day, channel ORDER BY day, channel')]
    return rows
