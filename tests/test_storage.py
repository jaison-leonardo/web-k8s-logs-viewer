import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from webapp.storage import LogFilter, Storage, normalize_ts


def rec(message, ts="2026-01-10T10:00:00.000000000Z", **kw):
    base = dict(environment="demo", namespace="ns", workload="orders", pod="orders-1", container="app",
                ts=ts, level="INFO", format="plain", logger=None, thread=None, trace_id=None,
                message=message, raw=message)
    base.update(kw)
    return base


@pytest.fixture
def storage(tmp_path):
    s = Storage(str(tmp_path / "logs.db"), retention_days=7, max_rows=0, start_threads=False)
    yield s
    s.close()


def test_normalize_ts():
    assert normalize_ts("2026-01-10T10:00:00.123456789Z") == "2026-01-10T10:00:00.123456789Z"
    assert normalize_ts("2026-01-10T10:00:00Z") == "2026-01-10T10:00:00.000000000Z"
    assert normalize_ts("2026-01-10T05:00:00.5-05:00") == "2026-01-10T10:00:00.500000000Z"
    assert normalize_ts("basura") is None


def test_dedup_on_reconnect(storage):
    first = storage.insert_many([rec("a"), rec("b", ts="2026-01-10T10:00:01.000000000Z")])
    again = storage.insert_many([rec("b", ts="2026-01-10T10:00:01.000000000Z"), rec("c", ts="2026-01-10T10:00:02.000000000Z")])
    assert len(first) == 2
    assert [r["message"] for r in again] == ["c"]
    assert storage.count(LogFilter()) == 3


def test_filters(storage):
    storage.insert_many([
        rec("Pedido creado", level="INFO", trace_id="t1"),
        rec("Falló el pago", level="ERROR", workload="payments", pod="payments-1",
            ts="2026-01-10T10:05:00.000000000Z", trace_id="t1"),
        rec("health check ok", level="DEBUG", ts="2026-01-10T10:10:00.000000000Z"),
    ])
    count = lambda **kw: storage.count(_flt(**kw))
    assert count(levels=["ERROR"]) == 1
    assert count(workloads=["orders"]) == 2
    assert count(text="PEDIDO") == 1
    assert count(text=r"^Fall[oó]", regex=True) == 1
    assert count(exclude="health") == 2
    assert count(trace_id="t1") == 2
    assert count(since="2026-01-10T10:04:00Z") == 2
    assert count(since="2026-01-10T05:04:00-05:00", until="2026-01-10T10:06:00Z") == 1


def _flt(**kw):
    flt = LogFilter(**kw)
    flt.validate()
    return flt


def test_live_matching_uses_same_semantics(storage):
    flt = _flt(levels=["ERROR"], text="pago", since="2026-01-10T10:00:00Z")
    assert flt.matches(rec("Falló el PAGO", level="ERROR", ts="2026-01-10T10:01:00.000000000Z"))
    assert not flt.matches(rec("Falló el pago", level="INFO", ts="2026-01-10T10:01:00.000000000Z"))
    assert not flt.matches(rec("Falló el pago", level="ERROR", ts="2026-01-10T09:59:00.000000000Z"))


def test_invalid_regex_is_rejected():
    import re
    with pytest.raises(re.error):
        LogFilter(text="([", regex=True).validate()


def test_query_order_and_iter(storage):
    # Insertados fuera de orden (como llega el historial inicial de varios pods).
    storage.insert_many([rec(f"m{i}", ts=f"2026-01-10T10:00:{i:02d}.000000000Z") for i in (3, 4, 0, 1, 2)])
    rows = storage.query(LogFilter(), limit=2, order="desc")
    assert [r["message"] for r in rows] == ["m4", "m3"]
    assert [r["message"] for r in storage.iter_rows(LogFilter(), batch=2)] == [f"m{i}" for i in range(5)]
    assert storage.distinct("workload") == ["orders"]
    with pytest.raises(ValueError):
        storage.distinct("message; DROP TABLE logs")


def test_retention_by_age_and_rows(tmp_path):
    s = Storage(str(tmp_path / "r.db"), retention_days=1, max_rows=3, start_threads=False)
    s.insert_many([rec(f"m{i}", ts=f"2026-01-10T10:00:{i:02d}.000000000Z") for i in range(5)])
    assert s.purge(now=datetime.now(timezone.utc)) == 2  # tope de filas
    assert s.purge(now=datetime.now(timezone.utc) + timedelta(days=2)) == 3  # antigüedad
    s.close()


def test_legacy_schema_is_preserved(tmp_path):
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, pod TEXT, message TEXT)")
    conn.execute("CREATE INDEX idx_logs_pod ON logs(pod)")
    conn.execute("INSERT INTO logs (pod, message) VALUES ('p', 'viejo')")
    conn.commit()
    conn.close()
    s = Storage(str(path), start_threads=False)
    assert s.insert_many([rec("nuevo")])
    s.close()
    conn = sqlite3.connect(path)
    assert conn.execute("SELECT message FROM logs_legacy_v0").fetchone()[0] == "viejo"
    conn.close()
