-- =========================
-- GOLD FEATURES (REBUILD)
-- =========================

TRUNCATE TABLE gold_features_player_day;

WITH day_sessions AS (
  SELECT
    player_id,
    DATE(started_at) AS date,
    COUNT(*) AS sessions_cnt,
    SUM(COALESCE(duration_seconds, 0)) / 60.0 AS session_minutes
  FROM fact_sessions
  GROUP BY 1,2
),
day_matches AS (
  SELECT
    mp.player_id,
    DATE(m.started_at) AS date,
    COUNT(DISTINCT mp.match_id) AS matches_played,
    SUM(CASE WHEN mp.left_early = TRUE THEN 1 ELSE 0 END) AS early_exits
  FROM fact_match_players mp
  JOIN fact_matches m ON m.match_id = mp.match_id
  GROUP BY 1,2
),
day_wl AS (
  SELECT
    mp.player_id,
    DATE(m.started_at) AS date,
    SUM(CASE WHEN m.winning_team IS NOT NULL AND mp.team = m.winning_team THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN m.winning_team IS NOT NULL AND mp.team <> m.winning_team THEN 1 ELSE 0 END) AS losses
  FROM fact_match_players mp
  JOIN fact_matches m ON m.match_id = mp.match_id
  GROUP BY 1,2
),
day_rewards AS (
  SELECT
    player_id,
    DATE(event_time) AS date,
    SUM(CASE WHEN reward_type = 'xp' THEN amount ELSE 0 END) AS xp_earned,
    SUM(CASE WHEN reward_type = 'gold' THEN amount ELSE 0 END) AS gold_earned
  FROM fact_rewards
  GROUP BY 1,2
),
day_purchases AS (
  SELECT
    player_id,
    DATE(event_time) AS date,
    COUNT(*) AS purchases_cnt,
    SUM(COALESCE(price, 0)) AS total_spent
  FROM fact_purchases
  GROUP BY 1,2
),
day_levels AS (
  SELECT
    player_id,
    DATE(event_time) AS date,
    SUM(GREATEST(new_level - old_level, 0)) AS level_gains
  FROM fact_levelups
  GROUP BY 1,2
),
players_days AS (
  SELECT player_id, date FROM day_sessions
  UNION
  SELECT player_id, date FROM day_matches
  UNION
  SELECT player_id, date FROM day_rewards
  UNION
  SELECT player_id, date FROM day_purchases
  UNION
  SELECT player_id, date FROM day_levels
),
max_day AS (
  SELECT MAX(date) AS max_date
  FROM players_days
),
label AS (
  SELECT
    pd.player_id,
    pd.date,
    CASE
      WHEN pd.date >= (SELECT max_date FROM max_day) THEN NULL
      WHEN EXISTS (
        SELECT 1
        FROM players_days pd2
        WHERE pd2.player_id = pd.player_id
          AND pd2.date > pd.date
          AND pd2.date <= pd.date + 1
      ) THEN 0
      ELSE 1
    END AS churn_7d
  FROM players_days pd
)
INSERT INTO gold_features_player_day (
  player_id, date,
  sessions_cnt, session_minutes,
  matches_played, wins, losses, early_exits,
  xp_earned, gold_earned,
  purchases_cnt, total_spent,
  level_gains,
  churn_7d
)
SELECT
  pd.player_id, pd.date,
  COALESCE(ds.sessions_cnt, 0),
  COALESCE(ds.session_minutes, 0),
  COALESCE(dm.matches_played, 0),
  COALESCE(dw.wins, 0),
  COALESCE(dw.losses, 0),
  COALESCE(dm.early_exits, 0),
  COALESCE(dr.xp_earned, 0),
  COALESCE(dr.gold_earned, 0),
  COALESCE(dp.purchases_cnt, 0),
  COALESCE(dp.total_spent, 0),
  COALESCE(dl.level_gains, 0),
  lb.churn_7d
FROM players_days pd
LEFT JOIN day_sessions ds ON ds.player_id = pd.player_id AND ds.date = pd.date
LEFT JOIN day_matches dm ON dm.player_id = pd.player_id AND dm.date = pd.date
LEFT JOIN day_wl dw ON dw.player_id = pd.player_id AND dw.date = pd.date
LEFT JOIN day_rewards dr ON dr.player_id = pd.player_id AND dr.date = pd.date
LEFT JOIN day_purchases dp ON dp.player_id = pd.player_id AND dp.date = pd.date
LEFT JOIN day_levels dl ON dl.player_id = pd.player_id AND dl.date = pd.date
JOIN label lb ON lb.player_id = pd.player_id AND lb.date = pd.date
WHERE lb.churn_7d IS NOT NULL
ORDER BY pd.date, pd.player_id;