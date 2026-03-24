import json
import psycopg2
from confluent_kafka import Consumer

# =========================
# DB CONNECTION
# =========================
conn = psycopg2.connect(
    host="localhost",
    database="eventsdb",
    user="app",
    password="app123"
)
conn.autocommit = True
cursor = conn.cursor()
print("Connected to Postgres")

# =========================
# KAFKA CONSUMER CONFIG
# =========================
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "silver-group-v1",     # אם תריץ שוב ורוצה לקרוא מהתחלה -> תחליף ל v2/v3
    "auto.offset.reset": "earliest"
}
consumer = Consumer(consumer_conf)
consumer.subscribe(["player-events", "match-events"])
print("Consumer started. Waiting for messages...")

def ts(s: str):
    # psycopg2 מקבל ISO טוב; נשאיר as-is
    return s

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        et = event.get("event_type")
        payload = event.get("payload", {}) or {}

        event_id = event.get("event_id")
        event_time = ts(event.get("event_time"))
        player_id = event.get("player_id")
        session_id = event.get("session_id")
        match_id = event.get("match_id")

        print(f"Processing: {et}")

        # -------------------------
        # player_session_started
        # -------------------------
        if et == "player_session_started":
            cursor.execute("""
                INSERT INTO fact_sessions (
                    session_id, player_id, started_at, entry_point
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (session_id) DO NOTHING
            """, (
                session_id,
                player_id,
                event_time,
                payload.get("entry_point")
            ))

        # -------------------------
        # player_session_ended
        # -------------------------
        elif et == "player_session_ended":
            cursor.execute("""
                UPDATE fact_sessions
                SET ended_at = %s,
                    end_reason = %s,
                    duration_seconds = EXTRACT(EPOCH FROM (%s::timestamp - started_at))::int
                WHERE session_id = %s
            """, (
                event_time,
                payload.get("end_reason"),
                event_time,
                session_id
            ))

        # -------------------------
        # match_started
        # -------------------------
        elif et == "match_started":
            cursor.execute("""
                INSERT INTO fact_matches (
                    match_id, started_at, game_mode, map_name, average_mmr, max_mmr_difference
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (match_id) DO NOTHING
            """, (
                match_id,
                event_time,
                payload.get("game_mode"),
                payload.get("map_name"),
                payload.get("average_mmr"),
                payload.get("max_mmr_difference")
            ))

            # upsert players list
            for p in payload.get("players", []) or []:
                cursor.execute("""
                    INSERT INTO fact_match_players (
                        match_id, player_id, team, party_id
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (match_id, player_id) DO UPDATE
                    SET team = EXCLUDED.team,
                        party_id = EXCLUDED.party_id
                """, (
                    match_id,
                    p.get("player_id"),
                    p.get("team"),
                    p.get("party_id")
                ))

        # -------------------------
        # match_ended
        # -------------------------
        elif et == "match_ended":
            cursor.execute("""
                UPDATE fact_matches
                SET ended_at = %s,
                    duration_seconds = %s,
                    winning_team = %s,
                    end_reason = %s
                WHERE match_id = %s
            """, (
                event_time,
                payload.get("duration_seconds"),
                payload.get("winning_team"),
                payload.get("end_reason"),
                match_id
            ))

            # update per-player stats
            for p in payload.get("players", []) or []:
                cursor.execute("""
                    INSERT INTO fact_match_players (
                        match_id, player_id, team, party_id, kills, deaths, assists, score, left_early
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (match_id, player_id) DO UPDATE
                    SET team = EXCLUDED.team,
                        party_id = EXCLUDED.party_id,
                        kills = EXCLUDED.kills,
                        deaths = EXCLUDED.deaths,
                        assists = EXCLUDED.assists,
                        score = EXCLUDED.score,
                        left_early = EXCLUDED.left_early
                """, (
                    match_id,
                    p.get("player_id"),
                    p.get("team"),
                    p.get("party_id"),
                    p.get("kills"),
                    p.get("deaths"),
                    p.get("assists"),
                    p.get("score"),
                    p.get("left_early")
                ))

        # -------------------------
        # reward_granted
        # -------------------------
        elif et == "reward_granted":
            cursor.execute("""
                INSERT INTO fact_rewards (
                    event_id, player_id, session_id, match_id,
                    reward_type, amount, reason, reason_id, balance_after,
                    event_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, (
                event_id, player_id, session_id, match_id,
                payload.get("reward_type"),
                payload.get("amount"),
                payload.get("reason"),
                payload.get("reason_id"),
                payload.get("balance_after"),
                event_time
            ))

        # -------------------------
        # item_purchased
        # -------------------------
        elif et == "item_purchased":
            cursor.execute("""
                INSERT INTO fact_purchases (
                    event_id, player_id, session_id, match_id,
                    item_id, item_category, price, currency_type, balance_after_purchase,
                    event_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, (
                event_id, player_id, session_id, match_id,
                payload.get("item_id"),
                payload.get("item_category"),
                payload.get("price"),
                payload.get("currency_type"),
                payload.get("balance_after_purchase"),
                event_time
            ))

        # -------------------------
        # level_up
        # -------------------------
        elif et == "level_up":
            cursor.execute("""
                INSERT INTO fact_levelups (
                    event_id, player_id, session_id, match_id,
                    old_level, new_level, total_xp,
                    event_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, (
                event_id, player_id, session_id, match_id,
                payload.get("old_level"),
                payload.get("new_level"),
                payload.get("total_xp"),
                event_time
            ))

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
    cursor.close()
    conn.close()