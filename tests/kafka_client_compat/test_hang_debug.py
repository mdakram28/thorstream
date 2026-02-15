"""Quick debug: see where we hang (produce vs consume). Run with broker already up."""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

def _bootstrap():
    return os.environ.get("THORSTREAM_BOOTSTRAP", "localhost:9093")

def test_produce_only():
    from kafka import KafkaProducer
    print("Creating producer...")
    p = KafkaProducer(
        bootstrap_servers=[_bootstrap()],
        request_timeout_ms=3000,
        api_version=(0, 10, 2),
    )
    print("Sending one message...")
    p.send("debug-topic", value=b"x")
    print("Flushing (timeout 3s)...")
    p.flush(timeout=3)
    print("Flush OK - produce path works")
    p.close()

if __name__ == "__main__":
    test_produce_only()
