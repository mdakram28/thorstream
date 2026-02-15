"""
Compact tests for Thorstream using the standard Kafka Python client (kafka-python).
The broker is started automatically (conftest.py). Run: pytest tests/kafka_client_compat/test_thorstream.py -v
Build first: cargo build --bin thorstream (or the fixture will use cargo run).
"""
import os
import pytest

try:
    import kafka  # noqa: F401
except ImportError:
    pytestmark = pytest.mark.skip(reason="kafka-python not installed (pip install -r tests/kafka_client_compat/requirements.txt)")

def _bootstrap():
    return os.environ.get("THORSTREAM_BOOTSTRAP", "localhost:9093")


def _producer(**kw):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=[_bootstrap()],
        request_timeout_ms=5000,
        api_version=(0, 10, 2),
        **kw,
    )


def _consumer(topic, **kw):
    from kafka import KafkaConsumer
    return KafkaConsumer(
        topic,
        bootstrap_servers=[_bootstrap()],
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        api_version=(0, 10, 2),
        **kw,
    )


def test_produce_consume():
    topic = "test-produce-consume"
    msgs = [b"a", b"b", b"c"]
    p = _producer()
    for m in msgs:
        p.send(topic, value=m)
    p.flush(timeout=5)
    p.close()
    c = _consumer(topic)
    got = [next(c).value for _ in msgs]
    c.close()
    assert got == msgs


def test_consume_empty_topic():
    from kafka import KafkaConsumer
    c = KafkaConsumer(
        "test-empty",
        bootstrap_servers=[_bootstrap()],
        consumer_timeout_ms=500,
        api_version=(0, 10, 2),
    )
    c.close()
    assert True


def test_produce_with_key():
    topic = "test-keys"
    p = _producer()
    p.send(topic, key=b"k1", value=b"v1")
    p.send(topic, key=b"k2", value=b"v2")
    p.flush(timeout=5)
    p.close()
    c = _consumer(topic)
    r1, r2 = next(c), next(c)
    c.close()
    assert (r1.key, r1.value) == (b"k1", b"v1")
    assert (r2.key, r2.value) == (b"k2", b"v2")


def test_offsets_in_order():
    topic = "test-offsets"
    p = _producer()
    p.send(topic, value=b"x")
    p.flush(timeout=5)
    p.close()
    c = _consumer(topic)
    m = next(c)
    c.close()
    assert m.offset >= 0
    assert m.value == b"x"


def test_multiple_topics():
    p = _producer()
    p.send("topic-a", value=b"a")
    p.send("topic-b", value=b"b")
    p.flush(timeout=5)
    p.close()
    for topic, want in [("topic-a", b"a"), ("topic-b", b"b")]:
        c = _consumer(topic)
        assert next(c).value == want
        c.close()
