import os, json, argparse
from datetime import datetime
from confluent_kafka import Consumer


def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def day_partition(event_time: str) -> str:
    # event_time אצלך ISO; ניקח YYYY-MM-DD
    return event_time[:10]


def write_event(base_dir: str, topic: str, e: dict):
    d = day_partition(e["event_time"])
    out_dir = os.path.join(base_dir, topic, f"dt={d}")
    ensure_dir(out_dir)
    path = os.path.join(out_dir, "events.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(e, ensure_ascii=False) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group", default="bronze-writer")
    ap.add_argument("--base_dir", default="data/bronze")
    ap.add_argument("--topics", nargs="+", default=["player-events", "match-events"])
    args = ap.parse_args()

    c = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    c.subscribe(args.topics)
    print(f"Bronze writer running. base_dir={args.base_dir}")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] {msg.error()}")
                continue
            e = json.loads(msg.value().decode("utf-8"))
            write_event(args.base_dir, msg.topic(), e)
    except KeyboardInterrupt:
        pass
    finally:
        c.close()


if __name__ == "__main__":
    main()