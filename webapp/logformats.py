"""Identificación y parseo de formatos de log.

Cada línea que llega de un contenedor se intenta reconocer contra una lista de
formatos conocidos (JSON, logfmt, Spring Boot, Logback/Log4j, Python, .NET, Go
klog/zap, accesos HTTP tipo nginx/apache) más los formatos personalizados que
se declaren en config.yaml. El formato reconocido se "pega" al stream para que
las siguientes líneas se prueben primero contra él, y las líneas que no
inician un registro (stack traces, mensajes de .NET en la línea siguiente,
etc.) se agregan al registro anterior en vez de quedar sueltas.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field

LEVELS = ["FATAL", "CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "UNKNOWN"]

# Formatos en los que cada línea es un registro completo (el stack trace, si
# existe, viaja dentro del propio JSON). Se emiten sin esperar continuaciones,
# lo que elimina la latencia de la ventana multilínea.
SINGLE_LINE_FORMATS = {"json", "logfmt", "http_access"}

_LEVEL_ALIASES = {
    "trace": "TRACE", "trce": "TRACE", "finest": "TRACE", "finer": "TRACE",
    "debug": "DEBUG", "dbug": "DEBUG", "fine": "DEBUG", "verbose": "DEBUG",
    "info": "INFO", "information": "INFO", "informational": "INFO", "notice": "INFO",
    "i": "INFO",
    "warn": "WARN", "warning": "WARN", "w": "WARN",
    "error": "ERROR", "err": "ERROR", "fail": "ERROR", "severe": "ERROR", "e": "ERROR",
    "critical": "CRITICAL", "crit": "CRITICAL",
    "fatal": "FATAL", "panic": "FATAL", "dpanic": "FATAL", "emerg": "FATAL",
    "emergency": "FATAL", "alert": "FATAL", "f": "FATAL",
}

# Niveles numéricos de pino/bunyan (Node.js).
_NUMERIC_LEVELS = [(60, "FATAL"), (50, "ERROR"), (40, "WARN"), (30, "INFO"), (20, "DEBUG"), (10, "TRACE")]


def normalize_level(value) -> str:
    if value is None:
        return "UNKNOWN"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        for threshold, level in _NUMERIC_LEVELS:
            if value >= threshold:
                return level
        return "TRACE"
    text = str(value).strip().lower()
    if text.isdigit():
        return normalize_level(int(text))
    return _LEVEL_ALIASES.get(text, "UNKNOWN")


_TRACE_IN_TEXT_RE = re.compile(
    r"(?:trace[_.-]?id|correlation[_.-]?id|x-request-id|request[_.-]?id)[\"']?\s*[=:]\s*[\"']?([A-Za-z0-9-]{8,64})",
    re.I,
)


def find_trace_id(text: str) -> str | None:
    match = _TRACE_IN_TEXT_RE.search(text)
    return match.group(1) if match else None


@dataclass
class LogRecord:
    format: str
    level: str
    message: str
    logger: str | None = None
    thread: str | None = None
    trace_id: str | None = None


class LogFormat:
    """Formato basado en una regex con grupos con nombre: level, message,
    logger, thread, trace_id (todos opcionales salvo message)."""

    structured = True

    def __init__(self, name: str, pattern: str | re.Pattern, level_from=None):
        self.name = name
        self.regex = re.compile(pattern) if isinstance(pattern, str) else pattern
        self.level_from = level_from

    def parse(self, text: str) -> LogRecord | None:
        match = self.regex.match(text)
        if not match:
            return None
        groups = match.groupdict()
        level = self.level_from(groups) if self.level_from else normalize_level(groups.get("level"))
        message = groups.get("message")
        if message is None:
            message = text
        trace_id = groups.get("trace_id") or find_trace_id(message)
        return LogRecord(
            format=self.name,
            level=level,
            message=message,
            logger=(groups.get("logger") or "").strip() or None,
            thread=(groups.get("thread") or "").strip() or None,
            trace_id=trace_id,
        )


# --------------------------------------------------------------------------
# Formatos estructurados (JSON, logfmt)
# --------------------------------------------------------------------------

def _lookup(data: dict, keys):
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return data[key]
        if "." in key:
            node = data
            for part in key.split("."):
                if not isinstance(node, dict) or part not in node:
                    node = None
                    break
                node = node[part]
            if node not in (None, ""):
                return node
    return None


class JsonFormat:
    name = "json"
    structured = True

    LEVEL_KEYS = ["level", "severity", "lvl", "log.level", "levelname", "loglevel", "@l", "Level"]
    MESSAGE_KEYS = ["message", "msg", "@m", "@mt", "log", "text", "Message", "MessageTemplate"]
    LOGGER_KEYS = ["logger", "logger_name", "loggerName", "log.logger", "name", "category",
                   "SourceContext", "Category"]
    THREAD_KEYS = ["thread", "thread_name", "threadName", "process.thread.name"]
    TRACE_KEYS = ["traceId", "trace_id", "traceid", "trace.id", "dd.trace_id", "X-B3-TraceId",
                  "correlationId", "correlation_id", "requestId", "request_id", "TraceId"]
    STACK_KEYS = ["stack_trace", "stackTrace", "stack", "exception", "error.stack_trace", "err.stack",
                  "Exception"]

    def parse(self, text: str) -> LogRecord | None:
        stripped = text.strip()
        if not (stripped.startswith("{") and stripped.endswith("}")):
            return None
        try:
            data = json.loads(stripped)
        except ValueError:
            return None
        if not isinstance(data, dict):
            return None
        message = _lookup(data, self.MESSAGE_KEYS)
        if isinstance(message, (dict, list)):
            message = json.dumps(message, ensure_ascii=False)
        message = str(message) if message is not None else stripped
        stack = _lookup(data, self.STACK_KEYS)
        if stack:
            message = f"{message}\n{stack if isinstance(stack, str) else json.dumps(stack, ensure_ascii=False)}"
        logger = _lookup(data, self.LOGGER_KEYS)
        thread = _lookup(data, self.THREAD_KEYS)
        trace = _lookup(data, self.TRACE_KEYS)
        return LogRecord(
            format=self.name,
            level=normalize_level(_lookup(data, self.LEVEL_KEYS)),
            message=message,
            logger=str(logger) if logger is not None else None,
            thread=str(thread) if thread is not None else None,
            trace_id=str(trace) if trace is not None else find_trace_id(message),
        )


class LogfmtFormat:
    name = "logfmt"
    structured = True
    PAIR_RE = re.compile(r'([\w.@-]+)=("(?:[^"\\]|\\.)*"|\S*)')

    def parse(self, text: str) -> LogRecord | None:
        pairs = {}
        for key, value in self.PAIR_RE.findall(text):
            if value.startswith('"') and value.endswith('"') and len(value) >= 2:
                value = value[1:-1].replace('\\"', '"')
            pairs[key] = value
        if len(pairs) < 2 or not ({"level", "lvl"} & pairs.keys()):
            return None
        message = pairs.get("msg") or pairs.get("message") or text
        trace = pairs.get("trace_id") or pairs.get("traceId") or pairs.get("traceID") or find_trace_id(text)
        return LogRecord(
            format=self.name,
            level=normalize_level(pairs.get("level") or pairs.get("lvl")),
            message=message,
            logger=pairs.get("logger") or pairs.get("caller") or pairs.get("component"),
            trace_id=trace,
        )


# --------------------------------------------------------------------------
# Formatos de texto
# --------------------------------------------------------------------------

_TS = r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
_LVL = r"TRACE|DEBUG|INFO|NOTICE|WARN|WARNING|ERROR|SEVERE|CRITICAL|FATAL"


class SpringBootFormat:
    """Patrón por defecto de Spring Boot 2.x y 3.x:
    2024-01-01T10:00:00.000-05:00  INFO 1 --- [app] [thread] [trace-span] logger : msg"""

    name = "spring_boot"
    structured = True
    REGEX = re.compile(
        rf"^(?P<time>{_TS})\s+(?P<level>{_LVL})\s+(?P<pid>\d+)\s+---\s+"
        r"(?P<brackets>(?:\[[^\]]*\]\s*)+)(?P<logger>\S+)\s+:\s(?P<message>.*)$"
    )
    BRACKET_RE = re.compile(r"\[([^\]]*)\]")
    # Micrometer Tracing: [traceId-spanId] con ids hexadecimales.
    CORRELATION_RE = re.compile(r"^[0-9a-fA-F]{16,32}-[0-9a-fA-F]{8,16}$")

    def parse(self, text: str) -> LogRecord | None:
        match = self.REGEX.match(text)
        if not match:
            return None
        brackets = [b.strip() for b in self.BRACKET_RE.findall(match.group("brackets"))]
        trace_id = None
        if brackets and (len(brackets) >= 3 or self.CORRELATION_RE.match(brackets[-1])):
            correlation = brackets.pop()
            trace_id = correlation.rsplit("-", 1)[0] if "-" in correlation else correlation
        # Quedan [app] [thread] (Spring Boot >= 3.2) o solo [thread].
        thread = brackets[-1] if brackets else None
        message = match.group("message")
        return LogRecord(
            format=self.name,
            level=normalize_level(match.group("level")),
            message=message,
            logger=match.group("logger"),
            thread=thread or None,
            trace_id=trace_id or find_trace_id(message),
        )


def _http_status_level(groups):
    status = int(groups.get("status") or 0)
    if status >= 500:
        return "ERROR"
    if status >= 400:
        return "WARN"
    return "INFO"


def _builtin_formats():
    return [
        JsonFormat(),
        SpringBootFormat(),
        LogFormat(
            "logback",
            rf"^(?P<time>{_TS})\s+\[(?P<thread>[^\]]+)\]\s+(?P<level>{_LVL})\s+(?P<logger>\S+)\s+-\s(?P<message>.*)$",
        ),
        LogFormat(
            "log4j",
            rf"^(?P<time>{_TS})\s+(?P<level>{_LVL})\s+\[(?P<thread>[^\]]+)\]\s+(?P<logger>\S+)\s*[-:]\s(?P<message>.*)$",
        ),
        LogFormat(
            "python",
            r"^(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\s+-\s+(?P<logger>\S+)\s+-\s+"
            r"(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+-\s+(?P<message>.*)$",
        ),
        LogFormat(
            "python_basic",
            r"^(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL):(?P<logger>[\w.]+):(?P<message>.*)$",
        ),
        LogFormat(
            "uvicorn",
            r"^(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL):\s+(?P<message>.*)$",
        ),
        LogFormat(
            "dotnet",
            r"^(?P<level>trce|dbug|info|warn|fail|crit): (?P<logger>[^\[\s]+)\[\d+\]\s*(?P<message>.*)$",
        ),
        LogFormat(
            "klog",
            r"^(?P<level>[IWEF])\d{4} \d{2}:\d{2}:\d{2}\.\d+\s+\d+ (?P<logger>[^\]]+)\] (?P<message>.*)$",
        ),
        LogFormat(
            "zap_console",
            r"^(?P<time>\d{4}-\d{2}-\d{2}T\S+)\t(?P<level>DEBUG|INFO|WARN|ERROR|DPANIC|PANIC|FATAL)\t"
            r"(?:(?P<logger>[^\t]+)\t)?(?P<message>.*)$",
        ),
        LogFormat(
            "http_access",
            # Sin grupo "message": se conserva la línea completa (incluye código y origen).
            r'^(?P<client>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] "(?P<request>[^"]*)" (?P<status>\d{3}) \S+.*$',
            level_from=_http_status_level,
        ),
        LogfmtFormat(),
        LogFormat(
            "generic",
            rf"^\[?(?P<time>{_TS})\]?\s+\[?(?P<level>{_LVL})\]?\s*[:\-]?\s*(?P<message>.*)$",
        ),
    ]


# --------------------------------------------------------------------------
# Texto plano (fallback) y continuaciones multilínea
# --------------------------------------------------------------------------

_PLAIN_LEVEL_RE = re.compile(rf"\b({_LVL})\b")
_EXCEPTION_HEAD_RE = re.compile(r"^(?:[\w$]+\.)+[\w$]*(?:Exception|Error)\b")

_CONTINUATION_RE = re.compile(
    r"^(?:\s+\S"                               # línea indentada
    r"|\s*at \S+"                              # frame de stack Java/JS
    r"|Caused by:"                             # causa encadenada Java
    r"|\s*\.\.\. \d+ (?:more|common frames omitted)"
    r"|Traceback \(most recent call last\)"    # Python
    r"|(?:[\w$]+\.)+[\w$]*(?:Exception|Error)\b"  # cabecera de excepción Java
    r"|\w+(?:Error|Exception): "               # última línea de traceback Python
    r"|---\s*End of)"                          # .NET inner exception
)


def plain_record(text: str) -> LogRecord:
    head = text[:150]
    match = _PLAIN_LEVEL_RE.search(head)
    if match:
        level = normalize_level(match.group(1))
    elif _EXCEPTION_HEAD_RE.match(text):
        level = "ERROR"
    else:
        level = "UNKNOWN"
    return LogRecord(format="plain", level=level, message=text, trace_id=find_trace_id(text))


class FormatRegistry:
    def __init__(self, custom=None):
        custom_formats = [LogFormat(c["name"], c["pattern"]) for c in (custom or [])]
        # Los personalizados van primero: el usuario los declara porque los conoce.
        self.formats = custom_formats + _builtin_formats()
        self.by_name = {f.name: f for f in self.formats}

    def names(self):
        return [f.name for f in self.formats] + ["plain"]

    def parse(self, text: str, preferred: str | None = None) -> LogRecord | None:
        if preferred and preferred in self.by_name:
            record = self.by_name[preferred].parse(text)
            if record:
                return record
        for fmt in self.formats:
            if fmt.name == preferred:
                continue
            record = fmt.parse(text)
            if record:
                return record
        return None


@dataclass
class Entry:
    """Un registro completo (posiblemente multilínea) listo para guardar."""
    ts: str | None
    record: LogRecord
    raw_lines: list = field(default_factory=list)

    @property
    def raw(self):
        return "\n".join(self.raw_lines)


class StreamParser:
    """Parser con estado para un stream (pod + contenedor).

    - Recuerda el último formato reconocido y lo prueba primero.
    - Si `forced_format` está definido (override en config), solo usa ese.
    - Agrupa las líneas de continuación con el registro anterior.
    """

    MAX_LINES_PER_ENTRY = 500

    def __init__(self, registry: FormatRegistry, forced_format: str | None = None, idle_seconds: float = 0.5):
        self.registry = registry
        self.forced_format = forced_format
        self.idle_seconds = idle_seconds
        self.preferred = forced_format
        self.counts: dict[str, int] = {}
        self._pending: Entry | None = None
        self._last_feed = 0.0
        # El historial inicial (tail_lines) puede empezar a mitad de un stack
        # trace cuya cabecera quedó fuera de la ventana: esas continuaciones
        # huérfanas iniciales se descartan (y se cuentan) en vez de quedar como
        # fragmentos sueltos sin nivel.
        self.skip_leading_orphans = True
        self.orphans_skipped = 0

    @property
    def dominant_format(self) -> str | None:
        if not self.counts:
            return None
        return max(self.counts.items(), key=lambda kv: kv[1])[0]

    def _stream_is_structured(self):
        fmt = self.dominant_format
        return fmt is not None and fmt != "plain"

    def _parse(self, text):
        if self.forced_format and self.forced_format != "auto":
            fmt = self.registry.by_name.get(self.forced_format)
            return fmt.parse(text) if fmt else None
        return self.registry.parse(text, self.preferred)

    def feed(self, ts: str | None, text: str, raw: str, now: float | None = None) -> list[Entry]:
        self._last_feed = now if now is not None else time.monotonic()
        record = self._parse(text)
        pending = self._pending

        if pending is not None and record is None and len(pending.raw_lines) < self.MAX_LINES_PER_ENTRY:
            if _CONTINUATION_RE.match(text) or self._stream_is_structured():
                if not pending.record.message.strip():
                    # .NET escribe "info: Categoria[0]" y el mensaje en la línea siguiente.
                    pending.record.message = text.strip()
                else:
                    pending.record.message = f"{pending.record.message}\n{text}"
                pending.raw_lines.append(raw)
                if not pending.record.trace_id:
                    pending.record.trace_id = find_trace_id(text)
                return []

        if record is None and pending is None and self.skip_leading_orphans and _CONTINUATION_RE.match(text):
            self.orphans_skipped += 1
            return []
        self.skip_leading_orphans = False

        if record is None:
            record = plain_record(text)
        else:
            self.preferred = record.format
        self.counts[record.format] = self.counts.get(record.format, 0) + 1

        done = [pending] if pending is not None else []
        entry = Entry(ts=ts, record=record, raw_lines=[raw])
        if record.format in SINGLE_LINE_FORMATS:
            self._pending = None
            done.append(entry)
        else:
            self._pending = entry
        return done

    def flush(self) -> list[Entry]:
        if self._pending is None:
            return []
        done, self._pending = [self._pending], None
        return done

    def flush_if_idle(self, now: float | None = None) -> list[Entry]:
        now = now if now is not None else time.monotonic()
        if self._pending is not None and now - self._last_feed >= self.idle_seconds:
            return self.flush()
        return []
