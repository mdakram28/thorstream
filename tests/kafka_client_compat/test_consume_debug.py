"""Quick debug: test consume path. Run with broker already up."""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

def _bootstrap():
    return os.environ.get("THORSTREAM_BOOTSTRAP", "localhost:9093")

def test_consume_only():
    from kafka import KafkaProducer, KafkaConsumer
    topic = "consume-debug"
    print("Creating producer...")
    p = KafkaProducer(
        bootstrap_servers=[_bootstrap()],
        request_timeout_ms=3000,
        api_version=(0, 10, 2),
    )
    print("Sending messages...")
    p.send(topic, value=b"msg1")
    p.send(topic, value=b"msg2")
    p.flush(timeout=3)
    p.close()
    print("Producer done. Creating consumer...")
    c = KafkaConsumer(
        topic,
        bootstrap_servers=[_bootstrap()],
        auto_offset_reset="earliest",
        consumer_timeout_ms=2000,
        api_version=(0, 10, 2),
    )
    print("Polling messages...")
    msgs = []
    for msg in c:
        print(f"Got message: {msg.value}")
        msgs.append(msg.value)
        if len(msgs) >= 2:
            break
    c.close()
    assert msgs == [b"msg1", b"msg2"], f"Expected [b'msg1', b'msg2'], got {msgs}"
    print("Consume path works!")

if __name__ == "__main__":
    test_consume_only()
