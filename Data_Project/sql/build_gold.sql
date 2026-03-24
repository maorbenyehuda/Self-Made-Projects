-- =========================
-- GOLD KPI TABLES (REBUILD)
-- =========================

TRUNCATE TABLE gold_daily_active_users;
TRUNCATE TABLE gold_session_length;
TRUNCATE TABLE gold_purchase_rate;
TRUNCATE TABLE gold_matches_per_player;
TRUNCATE TABLE gold_match_completion;
TRUNCATE TABLE gold_early_exit_rate;
TRUNCATE TABLE gold_match_balance;
TRUNCATE TABLE gold_progression_speed;
TRUNCATE TABLE gold_retention_d1_d7;

-- 1) DAU
INSERT INTO gold_daily_active_users (date, dau)
SELECT
  DATE(started_at) AS date,
  COUNT(DISTINCT player_id) AS dau
FROM fact_sessions
GROUP BY 1
ORDER BY 1;

-- 2) Session Length (minutes)
INSERT INTO gold_session_length (date, avg_session_minutes)
SELECT
  DATE(started_at) AS date,
  AVG(duration_seconds) / 60.0 AS avg_session_minutes
FROM fact_sessions
WHERE duration_seconds IS NOT NULL AND duration_seconds >= 0
GROUP BY 1
ORDER BY 1;

-- 3) Purchase Rate (buyers / dau)
INSERT INTO gold_purchase_rate (date, purchase_rate)
WITH dau AS (
  SELECT DATE(started_at) AS date, COUNT(DISTINCT player_id) AS dau
  FROM fact_sessions
  GROUP BY 1
),
buyers AS (
  SELECT DATE(event_time) AS date, COUNT(DISTINCT player_id) AS buyers
  FROM fact_purchases
  GROUP BY 1
)
SELECT
  d.date,
  COALESCE(b.buyers, 0)::float / NULLIF(d.dau, 0) AS purchase_rate
FROM dau d
LEFT JOIN buyers b ON b.date = d.date
ORDER BY 1;

-- 4) Matches per player (match participants / dau)
INSERT INTO gold_matches_per_player (date, matches_per_player)
WITH dau AS (
  SELECT DATE(started_at) AS date, COUNT(DISTINCT player_id) AS dau
  FROM fact_sessions
  GROUP BY 1
),
participants AS (
  SELECT DATE(m.started_at) AS date, COUNT(*) AS participant_rows
  FROM fact_match_players mp
  JOIN fact_matches m ON m.match_id = mp.match_id
  GROUP BY 1
)
SELECT
  d.date,
  COALESCE(p.participant_rows, 0)::float / NULLIF(d.dau, 0) AS matches_per_player
FROM dau d
LEFT JOIN participants p ON p.date = d.date
ORDER BY 1;

-- 5) Match completion (end_reason='normal')
INSERT INTO gold_match_completion (date, completion_rate)
SELECT
  DATE(started_at) AS date,
  AVG(CASE WHEN end_reason = 'normal' THEN 1 ELSE 0 END)::float AS completion_rate
FROM fact_matches
WHERE ended_at IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- 6) Early exit rate (left_early among match players)
INSERT INTO gold_early_exit_rate (date, early_exit_rate)
SELECT
  DATE(m.started_at) AS date,
  AVG(CASE WHEN mp.left_early = TRUE THEN 1 ELSE 0 END)::float AS early_exit_rate
FROM fact_match_players mp
JOIN fact_matches m ON m.match_id = mp.match_id
GROUP BY 1
ORDER BY 1;

-- 7) Match balance score = avg(abs(team1_score - team2_score))
INSERT INTO gold_match_balance (date, avg_score_diff)
WITH per_match AS (
  SELECT
    m.match_id,
    DATE(m.started_at) AS date,
    ABS(
      COALESCE(SUM(CASE WHEN mp.team = 1 THEN mp.score ELSE 0 END), 0)
      -
      COALESCE(SUM(CASE WHEN mp.team = 2 THEN mp.score ELSE 0 END), 0)
    ) AS score_diff
  FROM fact_matches m
  JOIN fact_match_players mp ON mp.match_id = m.match_id
  GROUP BY 1,2
)
SELECT date, AVG(score_diff)::float AS avg_score_diff
FROM per_match
GROUP BY 1
ORDER BY 1;

-- 8) Progression speed = avg(level gains per hour played)
INSERT INTO gold_progression_speed (date, avg_levels_per_hour)
WITH playtime AS (
  SELECT DATE(started_at) AS date, SUM(duration_seconds) / 3600.0 AS hours
  FROM fact_sessions
  WHERE duration_seconds IS NOT NULL AND duration_seconds > 0
  GROUP BY 1
),
levels AS (
  SELECT DATE(event_time) AS date, SUM(GREATEST(new_level - old_level, 0)) AS level_gains
  FROM fact_levelups
  GROUP BY 1
)
SELECT
  p.date,
  COALESCE(l.level_gains, 0)::float / NULLIF(p.hours, 0) AS avg_levels_per_hour
FROM playtime p
LEFT JOIN levels l ON l.date = p.date
ORDER BY 1;

-- 9) Retention D1/D7 using first session date as "registration"
INSERT INTO gold_retention_d1_d7 (registration_date, d1_retention, d7_retention)
WITH reg AS (
  SELECT player_id, MIN(DATE(started_at)) AS registration_date
  FROM fact_sessions
  GROUP BY 1
),
activity AS (
  SELECT DISTINCT player_id, DATE(started_at) AS activity_date
  FROM fact_sessions
),
flags AS (
  SELECT
    r.registration_date,
    r.player_id,
    CASE WHEN EXISTS (
      SELECT 1 FROM activity a
      WHERE a.player_id = r.player_id AND a.activity_date = r.registration_date + INTERVAL '1 day'
    ) THEN 1 ELSE 0 END AS d1,
    CASE WHEN EXISTS (
      SELECT 1 FROM activity a
      WHERE a.player_id = r.player_id AND a.activity_date = r.registration_date + INTERVAL '7 day'
    ) THEN 1 ELSE 0 END AS d7
  FROM reg r
)
SELECT
  registration_date,
  AVG(d1)::float AS d1_retention,
  AVG(d7)::float AS d7_retention
FROM flags
GROUP BY 1
ORDER BY 1;