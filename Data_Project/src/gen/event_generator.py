import argparse
import json
import os
import random
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional


# ----------------------------
# Helpers
# ----------------------------
# Convert the datetime to the next format: 2026-02-27T10:00:00Z
def iso_z(dt: datetime) -> str:
    """Return ISO8601 with Z."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# My events template contain things like | which means to pick only one
def pick_enum(s: str, rng: random.Random) -> str:
    """
    Templates contain enums like: "ranked | casual".
    Pick one option.
    """
    if isinstance(s, str) and " | " in s:
        options = [x.strip() for x in s.split("|")]
        return rng.choice(options)
    return s

# Do pick_enum inside the payload
def deep_pick_enums(obj, rng: random.Random):
    """Recursively replace enum-like strings in payload."""
    if isinstance(obj, dict):
        return {k: deep_pick_enums(v, rng) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_pick_enums(x, rng) for x in obj]
    if isinstance(obj, str):
        return pick_enum(obj, rng)
    return obj

# Formats player ID.
def format_player_id(i: int) -> str:
    # p_00001
    return f"p_{i:05d}"

# Generates session ID.
def make_session_id(rng: random.Random) -> str:
    return f"s_{rng.randint(10000, 99999)}"

# Genrates match ID.
def make_match_id(rng: random.Random) -> str:
    return f"m_{rng.randint(10000, 99999)}"

# This loads event templates from JSON files.
def load_templates(schema_dir: str) -> Dict[str, dict]:
    templates = {}
    for fn in os.listdir(schema_dir):
        if fn.endswith(".json"):
            path = os.path.join(schema_dir, fn)
            with open(path, "r", encoding="utf-8") as f:
                templates[fn.replace(".json", "")] = json.load(f)
    required = {
        "player_session_started",
        "player_session_ended",
        "match_started",
        "match_ended",
        "reward_granted",
        "level_up",
        "item_purchased",
    }
    missing = required - set(templates.keys())
    if missing:
        raise FileNotFoundError(f"Missing templates in {schema_dir}: {sorted(missing)}")
    return templates


# ----------------------------
# State
# ----------------------------
#This stores current player state during generation.
@dataclass
class PlayerState:
    player_id: str
    session_id: Optional[str] = None
    xp: int = 0
    level: int = 1
    gold: int = 0
    premium: int = 0  # optional currency for purchases


# ----------------------------
# Generator
# ----------------------------


class EventGenerator:
    def __init__(
        self,
        templates: Dict[str, dict],
        rng_seed: int = 42,
        game_version: str = "1.0.0",
        region: str = "eu",
    ):
        self.t = templates
        self.rng = random.Random(rng_seed)
        self.game_version = game_version
        self.region = region

        # very simple leveling curve (can tune later)
        # level 1->2 at 100 xp, 2->3 at 250, etc.
        self.level_thresholds = [0, 100, 250, 450, 700, 1000, 1350, 1750, 2200]

    # Going through the level thresholds, and return the current level by the xp
    def level_from_xp(self, xp: int) -> int:
        lvl = 1
        for i, thr in enumerate(self.level_thresholds, start=1):
            if xp >= thr:
                lvl = i
        return lvl

    
    def base_event(self, template_name: str, event_time: datetime) -> dict:
        e = deepcopy(self.t[template_name])
        e["event_id"] = str(uuid.uuid4())
        e["event_time"] = iso_z(event_time)

        # enforce shared fields
        e["game_version"] = self.game_version
        e["region"] = self.region

        # pick enums inside payload (like "ranked | casual")
        e["payload"] = deep_pick_enums(e.get("payload", {}), self.rng)
        return e

    # -------- player events --------

    def make_session_started(self, p: PlayerState, now: datetime) -> dict:
        e = self.base_event("player_session_started", now)
        p.session_id = make_session_id(self.rng)
        e["player_id"] = p.player_id
        e["session_id"] = p.session_id
        e["match_id"] = None
        # template already has source/client & platform/pc - keep it as is
        return e

    def make_session_ended(self, p: PlayerState, now: datetime) -> dict:
        e = self.base_event("player_session_ended", now)
        e["player_id"] = p.player_id
        e["session_id"] = p.session_id
        e["match_id"] = None
        p.session_id = None
        return e

    def make_reward_granted(
        self,
        p: PlayerState,
        now: datetime,
        match_id: str,
        reward_type: Optional[str] = None,
        amount: Optional[int] = None,
    ) -> dict:
        e = self.base_event("reward_granted", now)
        e["player_id"] = p.player_id
        e["session_id"] = p.session_id
        e["match_id"] = match_id

        rt = reward_type or self.rng.choice(["xp", "gold"])  # schema allows xp|gold
        amt = amount if amount is not None else self.rng.randint(20, 180)

        e["payload"]["reward_type"] = rt
        e["payload"]["amount"] = amt
        # set reason to match_reward and link match_id
        e["payload"]["reason"] = "match_reward"
        e["payload"]["reason_id"] = match_id

        if rt == "xp":
            p.xp += amt
            e["payload"]["balance_after"] = p.xp  # total_xp after
        else:
            p.gold += amt
            e["payload"]["balance_after"] = p.gold

        return e

    def make_level_up(self, p: PlayerState, now: datetime, match_id: str, old_level: int, new_level: int) -> dict:
        e = self.base_event("level_up", now)
        e["player_id"] = p.player_id
        e["session_id"] = p.session_id
        e["match_id"] = match_id
        e["payload"]["old_level"] = old_level
        e["payload"]["new_level"] = new_level
        e["payload"]["total_xp"] = p.xp
        return e

    def maybe_purchase(self, p: PlayerState, now: datetime) -> Optional[dict]:
        # small probability purchase
        if self.rng.random() > 0.12:
            return None

        e = self.base_event("item_purchased", now)
        e["player_id"] = p.player_id
        e["session_id"] = p.session_id
        e["match_id"] = None

        currency = self.rng.choice(["gold", "premium"])
        e["payload"]["currency_type"] = currency

        # choose price so it often "makes sense"
        if currency == "gold":
            price = self.rng.choice([50, 120, 250, 400])
            # ensure enough
            if p.gold < price:
                p.gold += price + self.rng.randint(10, 80)  # top-up for realism
            p.gold -= price
            e["payload"]["price"] = price
            e["payload"]["balance_after_purchase"] = p.gold
        else:
            price = self.rng.choice([5, 10, 25, 40])
            if p.premium < price:
                p.premium += price + self.rng.randint(1, 8)
            p.premium -= price
            e["payload"]["price"] = price
            e["payload"]["balance_after_purchase"] = p.premium

        return e

    # -------- match events --------

    def make_match_started(self, now: datetime, match_id: str, players: List[Tuple[PlayerState, int, str]]) -> dict:
        e = self.base_event("match_started", now)
        e["player_id"] = None
        e["session_id"] = None
        e["match_id"] = match_id

        # overwrite players list in payload with our IDs/teams/party
        e["payload"]["players"] = [
            {"player_id": ps.player_id, "team": team, "party_id": party_id}
            for (ps, team, party_id) in players
        ]

        # keep other fields but make them slightly variable
        e["payload"]["average_mmr"] = self.rng.randint(900, 1700)
        e["payload"]["max_mmr_difference"] = self.rng.randint(50, 400)
        return e

    def make_match_ended(
        self,
        start_time: datetime,
        end_time: datetime,
        match_id: str,
        players: List[Tuple[PlayerState, int, str]],
        winning_team: int,
    ) -> dict:
        e = self.base_event("match_ended", end_time)
        e["player_id"] = None
        e["session_id"] = None
        e["match_id"] = match_id

        duration = int((end_time - start_time).total_seconds())
        e["payload"]["duration_seconds"] = duration
        e["payload"]["winning_team"] = winning_team

        # build per-player stats
        plist = []
        for (ps, team, party_id) in players:
            kills = self.rng.randint(0, 14)
            deaths = self.rng.randint(0, 14)
            assists = self.rng.randint(0, 10)
            left_early = (self.rng.random() < 0.08)
            score = kills * 120 + assists * 60 - deaths * 20 + self.rng.randint(0, 120)

            plist.append({
                "player_id": ps.player_id,
                "team": team,
                "party_id": party_id,
                "kills": kills,
                "deaths": deaths,
                "assists": assists,
                "score": max(0, score),
                "left_early": left_early,
            })

        e["payload"]["players"] = plist
        return e

    # -------- main generation --------

    def generate(
        self,
        num_players: int,
        num_matches: int,
        start_time: datetime,
    ) -> Tuple[List[dict], List[dict]]:
        players = [PlayerState(player_id=format_player_id(i + 1)) for i in range(num_players)]

        player_events: List[dict] = []
        match_events: List[dict] = []

        now = start_time

        # 1) start sessions for all players (or most)
        for p in players:
            if self.rng.random() < 0.85:
                player_events.append(self.make_session_started(p, now))
                now += timedelta(seconds=self.rng.randint(2, 12))

        # 2) generate matches
        for _ in range(num_matches):
            # choose 2v2 or 3v3 (must be even)
            team_size = self.rng.choice([2, 3])
            k = team_size * 2

            # pick only players with active session; if not enough, start sessions
            active = [p for p in players if p.session_id is not None]
            while len(active) < k:
                p = self.rng.choice(players)
                if p.session_id is None:
                    player_events.append(self.make_session_started(p, now))
                    now += timedelta(seconds=self.rng.randint(2, 10))
                active = [pp for pp in players if pp.session_id is not None]

            chosen = self.rng.sample(active, k)
            match_id = make_match_id(self.rng)

            # teams + parties
            # keep parties simple: some pairs share party_id
            parties = ["party_a", "party_b", "party_c", "party_d"]
            self.rng.shuffle(parties)

            lineup: List[Tuple[PlayerState, int, str]] = []
            for idx, p in enumerate(chosen):
                team = 1 if idx < team_size else 2
                party_id = parties[idx % len(parties)]
                lineup.append((p, team, party_id))

            match_start = now
            match_events.append(self.make_match_started(match_start, match_id, lineup))

            # match duration
            dur = timedelta(seconds=self.rng.randint(420, 1200))
            match_end = match_start + dur
            winning_team = self.rng.choice([1, 2])
            match_events.append(self.make_match_ended(match_start, match_end, match_id, lineup, winning_team))

            # after match end: rewards (+ maybe level up)
            now = match_end + timedelta(seconds=self.rng.randint(1, 8))

            for (p, team, _) in lineup:
                old_level = p.level

                # reward xp always, gold sometimes (to create two events sometimes)
                player_events.append(self.make_reward_granted(p, now, match_id, reward_type="xp"))
                now += timedelta(seconds=self.rng.randint(1, 4))

                if self.rng.random() < 0.65:
                    player_events.append(self.make_reward_granted(p, now, match_id, reward_type="gold"))
                    now += timedelta(seconds=self.rng.randint(1, 4))

                # level up check
                new_level = self.level_from_xp(p.xp)
                if new_level > old_level:
                    p.level = new_level
                    player_events.append(self.make_level_up(p, now, match_id, old_level, new_level))
                    now += timedelta(seconds=self.rng.randint(1, 4))

                # optional purchase
                maybe = self.maybe_purchase(p, now)
                if maybe:
                    player_events.append(maybe)
                    now += timedelta(seconds=self.rng.randint(1, 6))

            # small gap between matches
            now += timedelta(seconds=self.rng.randint(10, 35))

        # 3) end sessions for some players
        for p in players:
            if p.session_id is not None and self.rng.random() < 0.75:
                player_events.append(self.make_session_ended(p, now))
                now += timedelta(seconds=self.rng.randint(2, 10))

        # Sort by event_time just to keep nice chronological files
        player_events.sort(key=lambda e: e["event_time"])
        match_events.sort(key=lambda e: e["event_time"])

        return player_events, match_events



def write_jsonl(path: str, events: List[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema_dir", default="schemas", help="Directory containing *.json templates")
    ap.add_argument("--out_dir", default="data/out", help="Output directory")
    ap.add_argument("--players", type=int, default=120)
    ap.add_argument("--matches", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start_time", default="2026-01-01T10:00:00Z")
    args = ap.parse_args()

    # parse start time
    # expects Z
    start_dt = datetime.strptime(args.start_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    templates = load_templates(args.schema_dir)
    gen = EventGenerator(templates, rng_seed=args.seed)

    player_events, match_events = gen.generate(
        num_players=args.players,
        num_matches=args.matches,
        start_time=start_dt,
    )

    player_path = os.path.join(args.out_dir, "player-events.jsonl")
    match_path = os.path.join(args.out_dir, "match-events.jsonl")

    write_jsonl(player_path, player_events)
    write_jsonl(match_path, match_events)

    print(f"Wrote {len(player_events)} player events -> {player_path}")
    print(f"Wrote {len(match_events)} match events  -> {match_path}")


if __name__ == "__main__":
    main()