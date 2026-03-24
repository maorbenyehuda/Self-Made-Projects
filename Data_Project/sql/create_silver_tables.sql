-- =========================
-- SILVER (FACT) TABLES
-- =========================

DROP TABLE IF EXISTS fact_match_players;
DROP TABLE IF EXISTS fact_levelups;
DROP TABLE IF EXISTS fact_purchases;
DROP TABLE IF EXISTS fact_rewards;
DROP TABLE IF EXISTS fact_matches;
DROP TABLE IF EXISTS fact_sessions;

-- 1) Sessions
CREATE TABLE fact_sessions (
    session_id TEXT PRIMARY KEY,
    player_id TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NULL,
    duration_seconds INTEGER NULL,
    entry_point TEXT NULL,
    end_reason TEXT NULL
);

-- 2) Matches
CREATE TABLE fact_matches (
    match_id TEXT PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NULL,
    duration_seconds INTEGER NULL,
    winning_team INTEGER NULL,
    end_reason TEXT NULL,
    game_mode TEXT NULL,
    map_name TEXT NULL,
    average_mmr INTEGER NULL,
    max_mmr_difference INTEGER NULL
);

-- 3) Match players (from payload.players)
CREATE TABLE fact_match_players (
    match_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    team INTEGER NULL,
    party_id TEXT NULL,
    kills INTEGER NULL,
    deaths INTEGER NULL,
    assists INTEGER NULL,
    score INTEGER NULL,
    left_early BOOLEAN NULL,
    PRIMARY KEY (match_id, player_id)
);

-- 4) Rewards
CREATE TABLE fact_rewards (
    event_id TEXT PRIMARY KEY,
    player_id TEXT NOT NULL,
    session_id TEXT NULL,
    match_id TEXT NULL,
    reward_type TEXT NULL,
    amount INTEGER NULL,
    reason TEXT NULL,
    reason_id TEXT NULL,
    balance_after INTEGER NULL,
    event_time TIMESTAMP NOT NULL
);

-- 5) Purchases
CREATE TABLE fact_purchases (
    event_id TEXT PRIMARY KEY,
    player_id TEXT NOT NULL,
    session_id TEXT NULL,
    match_id TEXT NULL,
    item_id TEXT NULL,
    item_category TEXT NULL,
    price INTEGER NULL,
    currency_type TEXT NULL,
    balance_after_purchase INTEGER NULL,
    event_time TIMESTAMP NOT NULL
);

-- 6) Level ups
CREATE TABLE fact_levelups (
    event_id TEXT PRIMARY KEY,
    player_id TEXT NOT NULL,
    session_id TEXT NULL,
    match_id TEXT NULL,
    old_level INTEGER NULL,
    new_level INTEGER NULL,
    total_xp INTEGER NULL,
    event_time TIMESTAMP NOT NULL
);