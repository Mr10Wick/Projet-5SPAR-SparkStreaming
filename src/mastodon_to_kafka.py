import json
import os
import re
import time
from typing import List, Dict, Any

from dotenv import load_dotenv
from confluent_kafka import Producer
from mastodon import Mastodon, StreamListener, MastodonError

load_dotenv()

BASE_URL = os.getenv("MASTODON_BASE_URL", "https://mastodon.social").rstrip("/")
ACCESS_TOKEN = (os.getenv("MASTODON_ACCESS_TOKEN") or "").strip()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mastodon_stream")

FILTER_LANGUAGE = (os.getenv("FILTER_LANGUAGE") or "").strip().lower()  # 'en', 'fr' ou vide
FILTER_KEYWORDS: List[str] = [
    kw.strip().lower() for kw in (os.getenv("FILTER_KEYWORDS") or "").split(",") if kw.strip()
]

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
    }

def passes_filters(rec: Dict[str, Any]) -> bool:
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
        print(f"[INFO] Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}", flush=True)

class PublicListener(StreamListener):
    def __init__(self, producer: Producer):
        super().__init__()
        self.producer = producer

    def on_update(self, status):
        try:
            rec = to_record(status)
            if passes_filters(rec):
                self.producer.produce(
                    KAFKA_TOPIC,
                    value=json.dumps(rec, ensure_ascii=False),
                    callback=delivery_report
                )
                # poll pour déclencher le callback
                self.producer.poll(0)
        except Exception as e:
            print(f"[WARN] on_update error: {e}", flush=True)

    def on_abort(self, err):
        print(f"[STREAM ABORT] {err}", flush=True)

    def on_error(self, err):
        print(f"[STREAM ERROR] {err}", flush=True)

def main():
    if not ACCESS_TOKEN:
        raise RuntimeError("MASTODON_ACCESS_TOKEN manquant dans .env")

    mastodon = Mastodon(api_base_url=BASE_URL, access_token=ACCESS_TOKEN)

    producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}
    producer = Producer(producer_conf)

    listener = PublicListener(producer)

    while True:
        try:
            print("[INFO] Connecting to Mastodon stream_public ...", flush=True)
            mastodon.stream_public(listener, run_async=False, reconnect_async=False, timeout=30)
        except MastodonError as me:
            print(f"[MastodonError] {me}; retry in 5s", flush=True)
            time.sleep(5)
        except Exception as e:
            print(f"[ERROR] {e}; retry in 5s", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
