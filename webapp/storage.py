"""Persistencia de logs en SQLite.

- Un único hilo escritor inserta por lotes (INSERT OR IGNORE sobre una clave de
  deduplicación) y notifica las filas realmente nuevas (para el WebSocket).
- Cada consulta abre su propia conexión de solo lectura (WAL permite leer
  mientras se escribe).
- Un hilo de retención purga por antigüedad y por número máximo de filas.
- `LogFilter` define la semántica de filtros, la misma para el histórico (SQL)
  y para el modo en vivo (`matches`).
"""

from __future__ import annotations

import hashlib
import logging
import os
import pathlib
import queue
import re
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache

log = logging.getLogger("pods.storage")

SCHEMA_VERSION = 2

_SCHEMA = """
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    environment TEXT NOT NULL,
    namespace TEXT NOT NULL,
    workload TEXT NOT NULL,
    pod TEXT NOT NULL,
    container TEXT NOT NULL,
    ts TEXT,
    level TEXT NOT NULL,
    format TEXT NOT NULL,
    logger TEXT,
    thread TEXT,
    trace_id TEXT,
    message TEXT NOT NULL,
    raw TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_logs_dedup ON logs(namespace, pod, container, ts, raw_hash);
CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts);
CREATE INDEX IF NOT EXISTS idx_logs_workload ON logs(workload);
CREATE INDEX IF NOT EXISTS idx_logs_pod ON logs(pod);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_trace ON logs(trace_id);
CREATE INDEX IF NOT EXISTS idx_logs_inserted ON logs(inserted_at);
"""

COLUMNS = ["id", "environment", "namespace", "workload", "pod", "container", "ts", "level",
           "format", "logger", "thread", "trace_id", "message", "raw", "inserted_at"]
_DISTINCT_ALLOWED = {"environment", "namespace", "workload", "pod", "container", "level", "format"}


def normalize_ts(value: str | None) -> str | None:
    """Lleva un timestamp ISO-8601 a UTC con 9 decimales ('...T12:00:00.123000000Z')
    para que la comparación como texto en SQLite sea cronológica."""
    if not value:
        return None
    text = value.strip()
    match = re.match(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:[.,](\d+))?(Z|[+-]\d{2}:?\d{2})?$", text)
    if not match:
        return None
    date, clock, frac, tz = match.groups()
    frac = (frac or "")[:9].ljust(9, "0")
    if tz in (None, "Z", "+00:00", "+0000"):
        return f"{date}T{clock}.{frac}Z"
    dt = datetime.fromisoformat(f"{date}T{clock}{tz if ':' in tz else tz[:3] + ':' + tz[3:]}")
    dt = dt.astimezone(timezone.utc)
    return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}.{frac}Z"


def parse_ts(value: str | None) -> datetime | None:
    norm = normalize_ts(value)
    if not norm:
        return None
    return datetime.strptime(norm[:26], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)


@lru_cache(maxsize=64)
def compile_regex(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.I)


def _sql_regexp(pattern, value):
    if value is None:
        return False
    try:
        return compile_regex(pattern).search(value) is not None
    except re.error:
        return False


@dataclass
class LogFilter:
    environments: list[str] = field(default_factory=list)
    namespaces: list[str] = field(default_factory=list)
    workloads: list[str] = field(default_factory=list)
    pods: list[str] = field(default_factory=list)
    containers: list[str] = field(default_factory=list)
    levels: list[str] = field(default_factory=list)
    formats: list[str] = field(default_factory=list)
    text: str | None = None
    regex: bool = False
    exclude: str | None = None
    trace_id: str | None = None
    logger: str | None = None
    since: str | None = None
    until: str | None = None

    def validate(self):
        if self.regex and self.text:
            compile_regex(self.text)  # lanza re.error si es inválida
        self.since = normalize_ts(self.since) if self.since else None
        self.until = normalize_ts(self.until) if self.until else None

    def where(self):
        clauses, params = [], []
        for column, values in (("environment", self.environments), ("namespace", self.namespaces),
                               ("workload", self.workloads), ("pod", self.pods),
                               ("container", self.containers), ("level", self.levels),
                               ("format", self.formats)):
            if values:
                clauses.append(f"{column} IN ({','.join('?' * len(values))})")
                params.extend(values)
        if self.text:
            if self.regex:
                clauses.append("message REGEXP ?")
                params.append(self.text)
            else:
                clauses.append("message LIKE ? ESCAPE '\\'")
                params.append(f"%{_escape_like(self.text)}%")
        if self.exclude:
            clauses.append("message NOT LIKE ? ESCAPE '\\'")
            params.append(f"%{_escape_like(self.exclude)}%")
        if self.trace_id:
            clauses.append("trace_id = ?")
            params.append(self.trace_id)
        if self.logger:
            clauses.append("logger LIKE ? ESCAPE '\\'")
            params.append(f"%{_escape_like(self.logger)}%")
        if self.since:
            clauses.append("COALESCE(ts, inserted_at) >= ?")
            params.append(self.since)
        if self.until:
            clauses.append("COALESCE(ts, inserted_at) <= ?")
            params.append(self.until)
        return (f"WHERE {' AND '.join(clauses)}" if clauses else ""), params

    def matches(self, item: dict) -> bool:
        for key, values in (("environment", self.environments), ("namespace", self.namespaces),
                            ("workload", self.workloads), ("pod", self.pods),
                            ("container", self.containers), ("level", self.levels),
                            ("format", self.formats)):
            if values and item.get(key) not in values:
                return False
        message = item.get("message") or ""
        if self.text:
            if self.regex:
                if not _sql_regexp(self.text, message):
                    return False
            elif self.text.lower() not in message.lower():
                return False
        if self.exclude and self.exclude.lower() in message.lower():
            return False
        if self.trace_id and item.get("trace_id") != self.trace_id:
            return False
        if self.logger and self.logger.lower() not in (item.get("logger") or "").lower():
            return False
        ts = item.get("ts") or item.get("inserted_at")
        if self.since and (not ts or ts < self.since):
            return False
        if self.until and (not ts or ts > self.until):
            return False
        return True


def _escape_like(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def raw_hash(raw: str) -> str:
    return hashlib.blake2b(raw.encode("utf-8", errors="replace"), digest_size=12).hexdigest()


class Storage:
    def __init__(self, db_path: str, retention_days: float = 7, max_rows: int = 2_000_000,
                 start_threads: bool = True):
        self.db_path = db_path
        self.retention_days = retention_days
        self.max_rows = max_rows
        self.on_inserted = None  # callback(list[dict]) con las filas nuevas
        self.dropped = 0
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._migrate()
        self._write_lock = threading.Lock()
        self._queue: queue.Queue = queue.Queue(maxsize=200_000)
        self._stop = threading.Event()
        self._threads = []
        if start_threads:
            for target, name in ((self._writer_loop, "storage-writer"), (self._retention_loop, "storage-retention")):
                thread = threading.Thread(target=target, name=name, daemon=True)
                thread.start()
                self._threads.append(thread)

    # ---------------------------------------------------------------- esquema
    def _migrate(self):
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        exists = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='logs'").fetchone()
        if exists and version < SCHEMA_VERSION:
            # Esquema del MVP anterior: se conserva aparte en vez de borrarlo.
            self._conn.execute(f"ALTER TABLE logs RENAME TO logs_legacy_v{version}")
            for (index_name,) in self._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_logs_%'").fetchall():
                self._conn.execute(f"DROP INDEX IF EXISTS {index_name}")
        self._conn.executescript(_SCHEMA)
        self._conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        self._conn.commit()

    # --------------------------------------------------------------- escritura
    def enqueue(self, record: dict):
        try:
            self._queue.put_nowait(record)
        except queue.Full:
            self.dropped += 1

    def insert_many(self, records: list[dict]) -> list[dict]:
        """Inserta y devuelve solo las filas nuevas (con id), ignorando duplicados."""
        inserted = []
        with self._write_lock:
            cur = self._conn.cursor()
            for rec in records:
                cur.execute(
                    "INSERT OR IGNORE INTO logs (environment, namespace, workload, pod, container, ts, level, "
                    "format, logger, thread, trace_id, message, raw, raw_hash) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (rec["environment"], rec["namespace"], rec["workload"], rec["pod"], rec["container"],
                     rec.get("ts"), rec["level"], rec["format"], rec.get("logger"), rec.get("thread"),
                     rec.get("trace_id"), rec["message"], rec["raw"], raw_hash(rec["raw"])),
                )
                if cur.rowcount == 1:
                    inserted.append({**rec, "id": cur.lastrowid})
            self._conn.commit()
        return inserted

    def _writer_loop(self):
        while not self._stop.is_set():
            try:
                first = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            batch = [first]
            while len(batch) < 1000:
                try:
                    batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            try:
                inserted = self.insert_many(batch)
            except sqlite3.Error:
                log.exception("Error insertando lote de %d registros", len(batch))
                continue
            if inserted and self.on_inserted:
                try:
                    self.on_inserted(inserted)
                except Exception:
                    log.exception("Error notificando registros nuevos")

    # --------------------------------------------------------------- retención
    def purge(self, now: datetime | None = None) -> int:
        now = now or datetime.now(timezone.utc)
        removed = 0
        with self._write_lock:
            if self.retention_days and self.retention_days > 0:
                cutoff = (now - timedelta(days=self.retention_days)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                removed += self._conn.execute("DELETE FROM logs WHERE inserted_at < ?", (cutoff,)).rowcount
            if self.max_rows and self.max_rows > 0:
                total = self._conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
                if total > self.max_rows:
                    removed += self._conn.execute(
                        "DELETE FROM logs WHERE id IN (SELECT id FROM logs ORDER BY id ASC LIMIT ?)",
                        (total - self.max_rows,),
                    ).rowcount
            self._conn.commit()
        return removed

    def _retention_loop(self):
        while not self._stop.wait(60 * 15):
            try:
                removed = self.purge()
                if removed:
                    log.info("Retención: %d registros eliminados", removed)
            except sqlite3.Error:
                log.exception("Error en la purga de retención")

    # ----------------------------------------------------------------- lectura
    def _reader(self):
        uri = pathlib.Path(os.path.abspath(self.db_path)).as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("PRAGMA busy_timeout=5000")
        conn.create_function("REGEXP", 2, _sql_regexp, deterministic=True)
        return conn

    def query(self, flt: LogFilter, limit=500, offset=0, order="desc") -> list[dict]:
        where, params = flt.where()
        order_sql = "ASC" if order == "asc" else "DESC"
        # Orden cronológico (no de llegada): al seguir varios pods el historial
        # inicial de cada uno llega en bloques.
        sql = (f"SELECT {', '.join(COLUMNS)} FROM logs {where} "
               f"ORDER BY ts {order_sql}, id {order_sql} LIMIT ? OFFSET ?")
        conn = self._reader()
        try:
            rows = conn.execute(sql, [*params, limit, offset]).fetchall()
        finally:
            conn.close()
        return [dict(zip(COLUMNS, row)) for row in rows]

    def count(self, flt: LogFilter) -> int:
        where, params = flt.where()
        conn = self._reader()
        try:
            return conn.execute(f"SELECT COUNT(*) FROM logs {where}", params).fetchone()[0]
        finally:
            conn.close()

    def iter_rows(self, flt: LogFilter, batch=2000):
        """Recorre todas las filas filtradas en orden, sin cargarlas en memoria."""
        where, params = flt.where()
        conn = self._reader()
        try:
            cur = conn.execute(f"SELECT {', '.join(COLUMNS)} FROM logs {where} ORDER BY ts ASC, id ASC", params)
            while True:
                rows = cur.fetchmany(batch)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(COLUMNS, row))
        finally:
            conn.close()

    def distinct(self, column: str) -> list[str]:
        if column not in _DISTINCT_ALLOWED:
            raise ValueError(column)
        conn = self._reader()
        try:
            rows = conn.execute(f"SELECT DISTINCT {column} FROM logs WHERE {column} IS NOT NULL ORDER BY 1")
            return [r[0] for r in rows.fetchall()]
        finally:
            conn.close()

    def close(self):
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=2)
        # Vaciar lo pendiente antes de cerrar.
        pending = []
        while True:
            try:
                pending.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if pending:
            self.insert_many(pending)
        self._conn.close()
