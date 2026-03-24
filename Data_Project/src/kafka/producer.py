import json
import argparse
from confluent_kafka import Producer


def route_event(e: dict) -> tuple[str, str]:
    et = e["event_type"]
    if et.startswith("match_"):
        return "match-events", e["match_id"]
    return "player-events", e["player_id"]


def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY ERROR] {err}")
    # else:
    #     print(f"[DELIVERED] {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def produce_jsonl(producer: Producer, path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            e = json.loads(line)
            topic, key = route_event(e)
            producer.produce(
                topic=topic,
                key=str(key),
                value=json.dumps(e, ensure_ascii=False),
                callback=delivery_report,
            )
            producer.poll(0)  # serve callbacks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--player_jsonl", default="data/out/player-events.jsonl")
    ap.add_argument("--match_jsonl", default="data/out/match-events.jsonl")
    ap.add_argument("--flush", type=int, default=10_000)
    args = ap.parse_args()

    p = Producer({"bootstrap.servers": args.bootstrap})

    # שולחים את שני הקבצים. (ה-route_event עדיין ינתב נכון, אבל זה גם ברור לוגית)
    produce_jsonl(p, args.player_jsonl)
    produce_jsonl(p, args.match_jsonl)

    p.flush(args.flush)
    print("Done producing.")


if __name__ == "__main__":
    main()