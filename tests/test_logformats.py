import pytest

from webapp.logformats import FormatRegistry, StreamParser, normalize_level, plain_record

REG = FormatRegistry()


@pytest.mark.parametrize("line, fmt, level, logger, trace", [
    # Spring Boot 2.x
    ("2026-01-10 10:00:00.123  INFO 1 --- [nio-8080-exec-1] c.e.orders.OrderController : Pedido recibido",
     "spring_boot", "INFO", "c.e.orders.OrderController", None),
    # Spring Boot 3.2+ con nombre de app y correlación de Micrometer
    ("2026-01-10T10:00:00.123-05:00 ERROR 1 --- [orders] [nio-8080-exec-1] "
     "[64f0a1c2b3d4e5f6a7b8c9d0e1f20314-a1b2c3d4e5f60718] c.e.orders.OrderService : Falló la reserva",
     "spring_boot", "ERROR", "c.e.orders.OrderService", "64f0a1c2b3d4e5f6a7b8c9d0e1f20314"),
    # JSON tipo Logback/ECS
    ('{"@timestamp":"2026-01-10T10:00:00Z","log.level":"warn","message":"Lento","log.logger":"db","trace.id":"abc123def456"}',
     "json", "WARN", "db", "abc123def456"),
    # JSON pino (Node.js) con nivel numérico
    ('{"level":50,"time":1700000000000,"name":"payments","msg":"Pago rechazado","traceId":"demo-00000006"}',
     "json", "ERROR", "payments", "demo-00000006"),
    # Logback por defecto
    ("2026-01-10 10:00:00,123 [main] DEBUG com.acme.App - Iniciando", "logback", "DEBUG", "com.acme.App", None),
    # Log4j
    ("2026-01-10 10:00:00,123 WARN [worker-1] com.acme.Job - Reintento", "log4j", "WARN", "com.acme.Job", None),
    # Python logging
    ("2026-01-10 10:00:00,123 - inventory.worker - WARNING - Stock bajo trace_id=demo-00000001",
     "python", "WARN", "inventory.worker", "demo-00000001"),
    ("ERROR:app.module:algo salió mal", "python_basic", "ERROR", "app.module", None),
    ("INFO:     127.0.0.1:5000 - \"GET / HTTP/1.1\" 200 OK", "uvicorn", "INFO", None, None),
    # .NET
    ("fail: Notifications.EmailSender[500]", "dotnet", "ERROR", "Notifications.EmailSender", None),
    # Go klog
    ("E0110 10:00:00.123456       1 controller.go:120] sync failed", "klog", "ERROR", "controller.go:120", None),
    # Go zap (consola)
    ("2026-01-10T10:00:00.000Z\tFATAL\tflaky\tconfiguración inválida", "zap_console", "FATAL", "flaky", None),
    # nginx/apache access log: el nivel sale del código HTTP
    ('10.0.0.1 - - [10/Jan/2026:10:00:00 +0000] "POST /api/orders HTTP/1.1" 502 512 "-" "curl/8"',
     "http_access", "ERROR", None, None),
    ('10.0.0.1 - - [10/Jan/2026:10:00:00 +0000] "GET /x HTTP/1.1" 404 0 "-" "curl/8"',
     "http_access", "WARN", None, None),
    # logfmt
    ('time=2026-01-10T10:00:00Z level=error msg="timeout llamando a pagos" trace_id=abcd1234',
     "logfmt", "ERROR", None, "abcd1234"),
    # Genérico: timestamp + nivel
    ("2026-01-10T10:00:00Z [WARN] disco al 90%", "generic", "WARN", None, None),
])
def test_known_formats(line, fmt, level, logger, trace):
    record = REG.parse(line)
    assert record is not None, line
    assert record.format == fmt
    assert record.level == level
    assert record.logger == logger
    assert record.trace_id == trace


def test_http_access_keeps_full_line():
    line = '10.0.0.1 - - [10/Jan/2026:10:00:00 +0000] "POST /api/orders HTTP/1.1" 502 512 "-" "curl/8"'
    assert REG.parse(line).message == line


def test_plain_fallback_does_not_flag_lowercase_business_words():
    # El MVP marcaba como ERROR cualquier mensaje con la palabra "error".
    assert REG.parse("usuario consultó el código de error de su factura") is None
    assert plain_record("usuario consultó el código de error de su factura").level == "UNKNOWN"
    assert plain_record("Something ERROR happened").level == "ERROR"
    assert plain_record("java.lang.NullPointerException: x").level == "ERROR"


def test_normalize_level_aliases():
    assert normalize_level("warning") == "WARN"
    assert normalize_level("SEVERE") == "ERROR"
    assert normalize_level(30) == "INFO"
    assert normalize_level("60") == "FATAL"
    assert normalize_level("raro") == "UNKNOWN"
    assert normalize_level(None) == "UNKNOWN"


def test_custom_format_has_priority():
    reg = FormatRegistry([{"name": "pipe", "pattern": r"^(?P<level>\w+) \| (?P<message>.*)$"}])
    record = reg.parse("ERROR | algo")
    assert record.format == "pipe" and record.level == "ERROR" and record.message == "algo"


def _feed_all(parser, lines):
    out = []
    for i, line in enumerate(lines):
        out.extend(parser.feed(f"2026-01-10T10:00:{i:02d}.000000000Z", line, line, now=float(i)))
    out.extend(parser.flush())
    return out


def test_java_stack_trace_is_grouped_and_keeps_level():
    lines = [
        "2026-01-10 10:00:00.000 ERROR 1 --- [main] c.e.Svc : Falló",
        "java.lang.IllegalStateException: boom",
        "\tat com.e.Svc.run(Svc.java:10)",
        "Caused by: java.io.IOException: x",
        "\t... 3 more",
        "2026-01-10 10:00:01.000  INFO 1 --- [main] c.e.Svc : Siguiente",
    ]
    entries = _feed_all(StreamParser(REG), lines)
    assert len(entries) == 2
    assert entries[0].record.level == "ERROR"
    assert entries[0].record.message.count("\n") == 4
    assert len(entries[0].raw_lines) == 5
    assert entries[1].record.message == "Siguiente"


def test_python_traceback_is_grouped():
    lines = [
        "2026-01-10 10:00:00,000 - w - ERROR - Falló",
        "Traceback (most recent call last):",
        '  File "/app/w.py", line 1, in f',
        "    x()",
        "TimeoutError: no respondió",
    ]
    entries = _feed_all(StreamParser(REG), lines)
    assert len(entries) == 1
    assert entries[0].record.message.endswith("TimeoutError: no respondió")


def test_dotnet_message_on_next_line():
    entries = _feed_all(StreamParser(REG), ["info: App.Worker[0]", "      Correo enviado"])
    assert len(entries) == 1
    assert entries[0].record.message == "Correo enviado"
    assert entries[0].record.level == "INFO"


def test_leading_orphan_continuations_are_skipped():
    # El historial inicial empieza a mitad de un stack trace (la cabecera quedó fuera).
    parser = StreamParser(REG)
    entries = _feed_all(parser, [
        "\tat com.e.Svc.run(Svc.java:10)",
        "Caused by: java.io.IOException: x",
        "2026-01-10 10:00:01.000  INFO 1 --- [main] c.e.Svc : ok",
        "2026-01-10 10:00:02.000 ERROR 1 --- [main] c.e.Svc : falla",
        "\tat com.e.Svc.run(Svc.java:10)",
    ])
    assert parser.orphans_skipped == 2
    assert [e.record.level for e in entries] == ["INFO", "ERROR"]
    assert entries[1].record.message.endswith("Svc.java:10)")  # las continuaciones posteriores sí se agrupan


def test_plain_stream_does_not_glue_unrelated_lines():
    entries = _feed_all(StreamParser(REG), ["linea uno", "linea dos"])
    assert [e.record.message for e in entries] == ["linea uno", "linea dos"]


def test_dominant_format_and_forced_format():
    parser = StreamParser(REG)
    _feed_all(parser, ['{"level":"info","msg":"a"}', '{"level":"info","msg":"b"}', "texto suelto"])
    assert parser.dominant_format == "json"

    forced = StreamParser(REG, forced_format="logfmt")
    entries = _feed_all(forced, ['{"level":"info","msg":"a"}'])
    assert entries[0].record.format == "plain"  # el JSON no se reconoce si se fuerza logfmt


def test_single_line_formats_are_emitted_immediately():
    parser = StreamParser(REG)
    out = parser.feed("t1", '{"level":"error","msg":"x"}', '{"level":"error","msg":"x"}', now=0.0)
    assert len(out) == 1 and out[0].record.level == "ERROR"
    assert parser.flush() == []
    # Los formatos de texto sí esperan por posibles líneas de continuación.
    assert parser.feed("t2", "2026-01-10 10:00:00,000 - w - ERROR - x", "r", now=1.0) == []


def test_flush_if_idle():
    parser = StreamParser(REG, idle_seconds=0.5)
    assert parser.feed(None, "hola", "hola", now=0.0) == []
    assert parser.flush_if_idle(now=0.2) == []
    assert len(parser.flush_if_idle(now=0.6)) == 1
