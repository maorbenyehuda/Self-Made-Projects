import os
from typing import Dict

import pandas as pd
import psycopg2
import streamlit as st


st.set_page_config(
    page_title="Gaming Analytics Dashboard",
    page_icon="🎮",
    layout="wide",
)


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "eventsdb"),
    "user": os.getenv("DB_USER", "app"),
    "password": os.getenv("DB_PASSWORD", "app123"),
}


@st.cache_resource(show_spinner=False)
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@st.cache_data(ttl=60, show_spinner=False)
def load_table(query: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(query, conn)


@st.cache_data(ttl=60, show_spinner=False)
def load_gold_tables() -> Dict[str, pd.DataFrame]:
    queries = {
        "dau": "SELECT date, dau FROM gold_daily_active_users ORDER BY date",
        "session_length": "SELECT date, avg_session_minutes FROM gold_session_length ORDER BY date",
        "purchase_rate": "SELECT date, purchase_rate FROM gold_purchase_rate ORDER BY date",
        "matches_per_player": "SELECT date, matches_per_player FROM gold_matches_per_player ORDER BY date",
        "match_completion": "SELECT date, completion_rate FROM gold_match_completion ORDER BY date",
        "early_exit_rate": "SELECT date, early_exit_rate FROM gold_early_exit_rate ORDER BY date",
        "match_balance": "SELECT date, avg_score_diff FROM gold_match_balance ORDER BY date",
        "progression_speed": "SELECT date, avg_levels_per_hour FROM gold_progression_speed ORDER BY date",
        "retention": "SELECT registration_date, d1_retention, d7_retention FROM gold_retention_d1_d7 ORDER BY registration_date",
        "features": "SELECT * FROM gold_features_player_day ORDER BY date, player_id",
    }
    return {name: load_table(query) for name, query in queries.items()}


@st.cache_data(ttl=60, show_spinner=False)
def load_daily_kpi_frame() -> pd.DataFrame:
    query = """
    WITH all_dates AS (
        SELECT date FROM gold_daily_active_users
        UNION
        SELECT date FROM gold_session_length
        UNION
        SELECT date FROM gold_purchase_rate
        UNION
        SELECT date FROM gold_matches_per_player
        UNION
        SELECT date FROM gold_match_completion
        UNION
        SELECT date FROM gold_early_exit_rate
        UNION
        SELECT date FROM gold_match_balance
        UNION
        SELECT date FROM gold_progression_speed
    )
    SELECT
        ad.date,
        d.dau,
        s.avg_session_minutes,
        p.purchase_rate,
        m.matches_per_player,
        c.completion_rate,
        e.early_exit_rate,
        b.avg_score_diff,
        g.avg_levels_per_hour
    FROM all_dates ad
    LEFT JOIN gold_daily_active_users d ON ad.date = d.date
    LEFT JOIN gold_session_length s ON ad.date = s.date
    LEFT JOIN gold_purchase_rate p ON ad.date = p.date
    LEFT JOIN gold_matches_per_player m ON ad.date = m.date
    LEFT JOIN gold_match_completion c ON ad.date = c.date
    LEFT JOIN gold_early_exit_rate e ON ad.date = e.date
    LEFT JOIN gold_match_balance b ON ad.date = b.date
    LEFT JOIN gold_progression_speed g ON ad.date = g.date
    ORDER BY ad.date
    """
    return load_table(query)


@st.cache_data(ttl=60, show_spinner=False)
def load_feature_summary() -> pd.DataFrame:
    query = """
    SELECT
        date,
        COUNT(DISTINCT player_id) AS players,
        AVG(sessions_cnt) AS avg_sessions_cnt,
        AVG(session_minutes) AS avg_session_minutes,
        AVG(matches_played) AS avg_matches_played,
        AVG(wins) AS avg_wins,
        AVG(losses) AS avg_losses,
        AVG(early_exits) AS avg_early_exits,
        AVG(xp_earned) AS avg_xp_earned,
        AVG(gold_earned) AS avg_gold_earned,
        AVG(purchases_cnt) AS avg_purchases_cnt,
        AVG(total_spent) AS avg_total_spent,
        AVG(level_gains) AS avg_level_gains,
        AVG(churn_7d::float) AS churn_rate
    FROM gold_features_player_day
    GROUP BY date
    ORDER BY date
    """
    return load_table(query)


@st.cache_data(ttl=60, show_spinner=False)
def load_churn_segments() -> pd.DataFrame:
    query = """
    SELECT
        churn_7d,
        COUNT(*) AS rows_cnt,
        AVG(session_minutes) AS avg_session_minutes,
        AVG(matches_played) AS avg_matches_played,
        AVG(early_exits) AS avg_early_exits,
        AVG(xp_earned) AS avg_xp_earned,
        AVG(purchases_cnt) AS avg_purchases_cnt,
        AVG(level_gains) AS avg_level_gains
    FROM gold_features_player_day
    GROUP BY churn_7d
    ORDER BY churn_7d
    """
    return load_table(query)


def format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value * 100:.1f}%"


def format_num(value: float | None, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:.{decimals}f}"


st.title("🎮 Gaming Analytics Dashboard")
st.caption("Gold KPIs + churn features powered by PostgreSQL and Streamlit")

with st.expander("What is Streamlit?"):
    st.markdown(
        """
        **Streamlit** is a Python framework for turning Python code into a web app quickly.

        Why it fits this project:
        - You already have Python and SQL.
        - You can query PostgreSQL directly.
        - You can build an internal dashboard without JavaScript.
        - It is perfect for portfolio projects, analytics demos, and ML prototypes.

        In practice, a Streamlit app is just a `.py` file that you run with:
        `streamlit run streamlit_dashboard.py`
        """
    )

with st.expander("How the app connects to Gold tables"):
    st.markdown(
        """
        Most Gold tables share the same key: **`date`**.

        That means the dashboard can join them like this:
        - `gold_daily_active_users`
        - `gold_session_length`
        - `gold_purchase_rate`
        - `gold_matches_per_player`
        - `gold_match_completion`
        - `gold_early_exit_rate`
        - `gold_match_balance`
        - `gold_progression_speed`

        They are merged with SQL using `LEFT JOIN ... USING (date)`.

        The retention table is different because its key is **`registration_date`**, not `date`,
        so it is visualized separately.

        The features table (`gold_features_player_day`) is also separate because it is
        at **player-day grain**, not day grain.
        """
    )

try:
    gold_tables = load_gold_tables()
    daily_kpis = load_daily_kpi_frame()
    feature_summary = load_feature_summary()
    churn_segments = load_churn_segments()
except Exception as exc:
    st.error("Failed to load data from PostgreSQL.")
    st.code(str(exc))
    st.stop()

if daily_kpis.empty:
    st.warning("No Gold KPI data found. Build the Gold tables first, then refresh the dashboard.")
    st.stop()

for col in ["date"]:
    daily_kpis[col] = pd.to_datetime(daily_kpis[col])
    feature_summary[col] = pd.to_datetime(feature_summary[col])

if not gold_tables["retention"].empty:
    gold_tables["retention"]["registration_date"] = pd.to_datetime(gold_tables["retention"]["registration_date"])
if not gold_tables["features"].empty:
    gold_tables["features"]["date"] = pd.to_datetime(gold_tables["features"]["date"])

min_date = daily_kpis["date"].min().date()
max_date = daily_kpis["date"].max().date()

start_date = st.sidebar.date_input(
    "Start date",
    value=min_date,
    min_value=min_date,
    max_value=max_date,
    key="start_date_input"
)

end_date = st.sidebar.date_input(
    "End date",
    value=max_date,
    min_value=min_date,
    max_value=max_date,
    key="end_date_input"
)

if start_date > end_date:
    st.sidebar.warning("Start date is after end date. Swapping them.")
    start_date, end_date = end_date, start_date

filtered_kpis = daily_kpis[
    (daily_kpis["date"].dt.date >= start_date)
    & (daily_kpis["date"].dt.date <= end_date)
].copy()

filtered_feature_summary = feature_summary[
    (feature_summary["date"].dt.date >= start_date)
    & (feature_summary["date"].dt.date <= end_date)
].copy()

latest = filtered_kpis.sort_values("date").iloc[-1]
latest_features = filtered_feature_summary.sort_values("date").iloc[-1] if not filtered_feature_summary.empty else None

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest DAU", int(latest["dau"]) if pd.notna(latest["dau"]) else 0)
col2.metric("Avg Session (min)", format_num(latest["avg_session_minutes"], 1))
col3.metric("Purchase Rate", format_pct(latest["purchase_rate"]))
col4.metric("Match Completion", format_pct(latest["completion_rate"]))

col5, col6, col7, col8 = st.columns(4)
col5.metric("Matches / Player", format_num(latest["matches_per_player"], 2))
col6.metric("Early Exit Rate", format_pct(latest["early_exit_rate"]))
col7.metric("Balance Score Diff", format_num(latest["avg_score_diff"], 2))
col8.metric("Levels / Hour", format_num(latest["avg_levels_per_hour"], 2))

if latest_features is not None:
    st.subheader("Feature Layer Snapshot")
    f1, f2, f3, f4 = st.columns(4)
    f1.metric("Players in Features", int(latest_features["players"]))
    f2.metric("Feature Churn Rate", format_pct(latest_features["churn_rate"]))
    f3.metric("Avg XP Earned", format_num(latest_features["avg_xp_earned"], 2))
    f4.metric("Avg Matches Played", format_num(latest_features["avg_matches_played"], 2))


tab1, tab2, tab3, tab4 = st.tabs([
    "Daily KPI Trends",
    "Retention",
    "Churn Features",
    "Raw Tables",
])
import plotly.express as px

with tab1:
    st.subheader("Daily KPI Trends")

    selectable_metrics = {
        "DAU": "dau",
        "Avg Session Minutes": "avg_session_minutes",
        "Purchase Rate": "purchase_rate",
        "Matches per Player": "matches_per_player",
        "Completion Rate": "completion_rate",
        "Early Exit Rate": "early_exit_rate",
        "Avg Score Diff": "avg_score_diff",
        "Avg Levels per Hour": "avg_levels_per_hour",
    }

    selected_metric_labels = st.multiselect(
        "Select metrics",
        list(selectable_metrics.keys()),
        default=["DAU", "Avg Session Minutes", "Purchase Rate"],
    )

    if selected_metric_labels:
        chart_df = filtered_kpis.copy()

        # Make sure date is parsed and sorted
        chart_df["date"] = pd.to_datetime(chart_df["date"])
        chart_df = chart_df.sort_values("date")

        # Convert date to string label so X-axis is shown day-by-day exactly
        chart_df["date_label"] = chart_df["date"].dt.strftime("%Y-%m-%d")

        metric_cols = [selectable_metrics[label] for label in selected_metric_labels]

        plot_df = chart_df.melt(
            id_vars=["date", "date_label"],
            value_vars=metric_cols,
            var_name="metric",
            value_name="value"
        )

        fig = px.line(
            plot_df,
            x="date_label",
            y="value",
            color="metric",
            markers=True,
            category_orders={"date_label": chart_df["date_label"].tolist()},
        )

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Value",
            legend_title="Metric",
            hovermode="x unified",
        )

        fig.update_xaxes(
            type="category",
            tickangle=45
        )

        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Daily KPI Table")
    st.dataframe(filtered_kpis, use_container_width=True)

with tab2:
    st.subheader("Retention Cohorts")
    retention_df = gold_tables["retention"].copy()
    if retention_df.empty:
        st.info("No retention rows found.")
    else:
        retention_df = retention_df[
            (retention_df["registration_date"].dt.date >= start_date)
            & (retention_df["registration_date"].dt.date <= end_date)
        ]
        st.line_chart(
            retention_df.set_index("registration_date")[["d1_retention", "d7_retention"]]
        )
        st.dataframe(retention_df, use_container_width=True)
        st.caption("Retention is shown separately because its grain is cohort date (`registration_date`), not daily activity date.")

with tab3:
    st.subheader("Churn Feature Trends")
    if filtered_feature_summary.empty:
        st.info("No feature summary rows found.")
    else:
        st.line_chart(
            filtered_feature_summary.set_index("date")[["churn_rate", "avg_session_minutes", "avg_matches_played"]]
        )
        st.dataframe(filtered_feature_summary, use_container_width=True)

    st.subheader("Churn Segment Comparison")
    st.dataframe(churn_segments, use_container_width=True)
    st.caption("This table compares average behavior for retained vs churned player-days.")

    st.subheader("Player-Day Explorer")
    features_df = gold_tables["features"].copy()
    if features_df.empty:
        st.info("No rows found in gold_features_player_day.")
    else:
        available_players = sorted(features_df["player_id"].dropna().unique().tolist())
        selected_players = st.multiselect(
            "Filter players",
            available_players,
            default=available_players[: min(5, len(available_players))],
        )

        player_filtered = features_df[
            (features_df["date"].dt.date >= start_date)
            & (features_df["date"].dt.date <= end_date)
        ]
        if selected_players:
            player_filtered = player_filtered[player_filtered["player_id"].isin(selected_players)]

        st.dataframe(player_filtered, use_container_width=True)

with tab4:
    st.subheader("Gold Tables")
    table_name = st.selectbox(
        "Choose table",
        [
            "dau",
            "session_length",
            "purchase_rate",
            "matches_per_player",
            "match_completion",
            "early_exit_rate",
            "match_balance",
            "progression_speed",
            "retention",
            "features",
        ],
    )
    st.dataframe(gold_tables[table_name], use_container_width=True)

st.caption(
    "Tip: if you rebuild SQL tables and do not see updates, click 'Rerun' in Streamlit or clear the cache."
)
