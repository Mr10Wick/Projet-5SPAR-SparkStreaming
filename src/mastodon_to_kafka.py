import json
import os
import re
import time
import signal
import random
from typing import List, Dict, Any

from dotenv import load_dotenv
from confluent_kafka import Producer
from mastodon import Mastodon, StreamListener, MastodonError

load_dotenv()

BASE_URL = os.getenv("MASTODON_BASE_URL", "https://mastodon.social").rstrip("/")
ACCESS_TOKEN = (os.getenv("MASTODON_ACCESS_TOKEN") or "").strip()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mastodon_stream")

# Filtres (laisser vide pour tout capter)
FILTER_LANGUAGE = (os.getenv("FILTER_LANGUAGE") or "").strip().lower()
FILTER_KEYWORDS: List[str] = [kw.strip().lower() for kw in (os.getenv("FILTER_KEYWORDS") or "").split(",") if kw.strip()]
EXCLUDE_REBLOGS = (os.getenv("EXCLUDE_REBLOGS", "true").lower() == "true")  # coupe le bruit

TAG_RE = re.compile(r"<[^>]+>")

def strip_html(html: str) -> str:
    return TAG_RE.sub("", html or "").strip()

def to_record(status: Dict[str, Any]) -> Dict[str, Any]:
    content = strip_html(status.get("content", ""))
    tags = [t.get("name", "").lower() for t in status.get("tags", []) if isinstance(t, dict)]
    acc = status.get("account", {}) or {}
    return {
        "id": status.get("id"),
        "created_at": str(status.get("created_at")),
        "language": status.get("language"),
        "text": content,
        "hashtags": tags,
        "user_id": acc.get("id"),
        "username": acc.get("acct"),
        "display_name": acc.get("display_name"),
        "favourites": status.get("favourites_count"),
        "reblogs": status.get("reblogs_count"),
        "replies": status.get("replies_count"),
        "url": status.get("url"),
        "reblog": bool(status.get("reblog")),  # True si c’est un boost
    }

def passes_filters(rec: Dict[str, Any]) -> bool:
    if EXCLUDE_REBLOGS and rec.get("reblog"):
        return False
    if FILTER_LANGUAGE and (rec.get("language") or "").lower() != FILTER_LANGUAGE:
        return False
    if FILTER_KEYWORDS:
        text_low = (rec.get("text") or "").lower()
        tags_low = rec.get("hashtags") or []
        if not any(k in text_low for k in FILTER_KEYWORDS) and not any(k in tags_low for k in FILTER_KEYWORDS):
            return False
    return True

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}", flush=True)
    else:
        pass  # trop verbeux en démo, on peut laisser silencieux

class PublicListener(StreamListener):
    def __init__(self, producer: Producer):
        super().__init__()
        self.producer = producer

    def on_update(self, status):
        try:
            rec = to_record(status)
            if passes_filters(rec):
                # clé Kafka = id du toot (string)
                key = str(rec.get("id") or "")
                payload = json.dumps(rec, ensure_ascii=False)
                self.producer.produce(
                    KAFKA_TOPIC,
                    key=key.encode("utf-8") if key else None,
                    value=payload.encode("utf-8"),
                    callback=delivery_report
                )
                self.producer.poll(0)
                # log court lisible live
                snippet = (rec.get("text") or "").replace("\n", " ")[:60]
                print(f"[TOOT] @{rec.get('username')}: {snippet}", flush=True)
        except Exception as e:
            print(f"[WARN] on_update error: {e}", flush=True)

    def on_abort(self, err):
        print(f"[STREAM ABORT] {err}", flush=True)

    def on_error(self, err):
        print(f"[STREAM ERROR] {err}", flush=True)

stop_flag = False
def _graceful_exit(signum, frame):
    global stop_flag
    stop_flag = True
    print("[INFO] Stopping… (signal received)", flush=True)

signal.signal(signal.SIGINT, _graceful_exit)
signal.signal(signal.SIGTERM, _graceful_exit)

def main():
    if not ACCESS_TOKEN:
        raise RuntimeError("MASTODON_ACCESS_TOKEN manquant dans .env")

    mastodon = Mastodon(api_base_url=BASE_URL, access_token=ACCESS_TOKEN)

    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 50,
        "acks": "1",
    }
    producer = Producer(producer_conf)
    listener = PublicListener(producer)

    backoff = 1.0
    while not stop_flag:
        try:
            print("[INFO] Connecting to Mastodon stream_public …", flush=True)
            # Flux public global de l’instance choisie
            mastodon.stream_public(listener, run_async=False, reconnect_async=False, timeout=30)
            backoff = 1.0  # si on sort proprement, on remet le backoff
        except MastodonError as me:
            print(f"[MastodonError] {me}; retry in {backoff:.1f}s", flush=True)
            time.sleep(backoff)
            backoff = min(60.0, backoff * 2 * (1 + random.random() * 0.2))  # jitter
        except Exception as e:
            print(f"[ERROR] {e}; retry in {backoff:.1f}s", flush=True)
            time.sleep(backoff)
            backoff = min(60.0, backoff * 2 * (1 + random.random() * 0.2))

    try:
        print("[INFO] Flushing producer…", flush=True)
        producer.flush(5)
    except Exception:
        pass

if __name__ == "__main__":
    main()