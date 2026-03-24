DROP TABLE IF EXISTS gold_daily_active_users;
DROP TABLE IF EXISTS gold_session_length;
DROP TABLE IF EXISTS gold_purchase_rate;
DROP TABLE IF EXISTS gold_matches_per_player;
DROP TABLE IF EXISTS gold_match_completion;
DROP TABLE IF EXISTS gold_early_exit_rate;
DROP TABLE IF EXISTS gold_match_balance;
DROP TABLE IF EXISTS gold_progression_speed;
DROP TABLE IF EXISTS gold_retention_d1_d7;

CREATE TABLE gold_daily_active_users (
    date DATE PRIMARY KEY,
    dau INTEGER
);

CREATE TABLE gold_session_length (
    date DATE PRIMARY KEY,
    avg_session_minutes FLOAT
);

CREATE TABLE gold_purchase_rate (
    date DATE PRIMARY KEY,
    purchase_rate FLOAT
);

CREATE TABLE gold_matches_per_player (
    date DATE PRIMARY KEY,
    matches_per_player FLOAT
);

CREATE TABLE gold_match_completion (
    date DATE PRIMARY KEY,
    completion_rate FLOAT
);

CREATE TABLE gold_early_exit_rate (
    date DATE PRIMARY KEY,
    early_exit_rate FLOAT
);

CREATE TABLE gold_match_balance (
    date DATE PRIMARY KEY,
    avg_score_diff FLOAT
);

CREATE TABLE gold_progression_speed (
    date DATE PRIMARY KEY,
    avg_levels_per_hour FLOAT
);

CREATE TABLE gold_retention_d1_d7 (
    registration_date DATE PRIMARY KEY,
    d1_retention FLOAT,
    d7_retention FLOAT
);