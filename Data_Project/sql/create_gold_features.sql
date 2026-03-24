DROP TABLE IF EXISTS gold_features_player_day;

CREATE TABLE gold_features_player_day (
    player_id TEXT,
    date DATE,
    sessions_cnt INTEGER,
    session_minutes FLOAT,
    matches_played INTEGER,
    wins INTEGER,
    losses INTEGER,
    early_exits INTEGER,
    xp_earned INTEGER,
    gold_earned INTEGER,
    purchases_cnt INTEGER,
    total_spent FLOAT,
    level_gains INTEGER,
    churn_7d INTEGER,
    PRIMARY KEY (player_id, date)
);