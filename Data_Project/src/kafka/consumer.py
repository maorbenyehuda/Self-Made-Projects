import json
import argparse
from confluent_kafka import Consumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group", default="verifier")
    ap.add_argument("--topics", nargs="+", default=["player-events", "match-events"])
    ap.add_argument("--auto_offset_reset", default="earliest")
    args = ap.parse_args()

    c = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": args.auto_offset_reset,
        "enable.auto.commit": True,
    })

    c.subscribe(args.topics)
    print(f"Subscribed to {args.topics} as group '{args.group}'")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] {msg.error()}")
                continue

            value = msg.value().decode("utf-8")
            e = json.loads(value)

            print(
                f"{msg.topic()} p{msg.partition()} off{msg.offset()} | "
                f"type={e.get('event_type')} key={msg.key().decode('utf-8') if msg.key() else None} "
                f"player_id={e.get('player_id')} match_id={e.get('match_id')} session_id={e.get('session_id')}"
            )

    except KeyboardInterrupt:
        pass
    finally:
        c.close()


if __name__ == "__main__":
    main()