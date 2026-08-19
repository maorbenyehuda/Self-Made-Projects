from pathlib import Path
import json

import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Los Angeles Crime Analytics",
    page_icon="🚔",
    layout="wide"
)


# ============================================================
# FILE PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# Support both plain CSV and compressed CSV
CLEAN_DATA_FILE = PROCESSED_DIR / "crime_clean.csv"
CLEAN_DATA_FILE_GZ = PROCESSED_DIR / "crime_clean.csv.gz"

QUALITY_FILE = PROCESSED_DIR / "data_quality_report.json"


# ============================================================
# DATA LOADING
# ============================================================

@st.cache_data
def load_data():

    if CLEAN_DATA_FILE.exists():
        file_path = CLEAN_DATA_FILE

    elif CLEAN_DATA_FILE_GZ.exists():
        file_path = CLEAN_DATA_FILE_GZ

    else:
        return None

    df = pd.read_csv(
        file_path,
        low_memory=False
    )

    # Convert dates
    if "date_occurred" in df.columns:
        df["date_occurred"] = pd.to_datetime(
            df["date_occurred"],
            errors="coerce"
        )

    if "date_reported" in df.columns:
        df["date_reported"] = pd.to_datetime(
            df["date_reported"],
            errors="coerce"
        )

    # Numeric fields
    if "victim_age" in df.columns:
        df["victim_age"] = pd.to_numeric(
            df["victim_age"],
            errors="coerce"
        )

    # Replace missing categorical values
    categorical_columns = [
        "area_name",
        "crime_type",
        "victim_sex",
        "victim_age_group"
    ]

    for column in categorical_columns:

        if column in df.columns:

            df[column] = (
                df[column]
                .fillna("Unknown")
                .astype(str)
            )

    return df


@st.cache_data
def load_quality_report():

    if not QUALITY_FILE.exists():
        return None

    with open(
        QUALITY_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(file)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def top_category(df, column):

    if df.empty:
        return None, 0

    result = (
        df.groupby(column)["crime_id"]
        .nunique()
        .sort_values(ascending=False)
    )

    if result.empty:
        return None, 0

    return result.index[0], int(result.iloc[0])


def calculate_yoy_change(df):

    yearly = (
        df.dropna(subset=["year"])
        .groupby("year")["crime_id"]
        .nunique()
        .sort_index()
    )

    if len(yearly) < 2:
        return None, None, None

    latest_year = yearly.index[-1]
    previous_year = yearly.index[-2]

    latest_count = yearly.iloc[-1]
    previous_count = yearly.iloc[-2]

    if previous_count == 0:
        return None, previous_year, latest_year

    change = (
        (latest_count - previous_count)
        / previous_count
        * 100
    )

    return change, previous_year, latest_year


# ============================================================
# LOAD DATA
# ============================================================

df = load_data()
quality_report = load_quality_report()


if df is None:

    st.error(
        "Processed data was not found. "
        "Run `python etl.py` before starting the dashboard."
    )

    st.stop()


if df.empty:

    st.error(
        "The processed dataset exists but contains no records."
    )

    st.stop()


# ============================================================
# HEADER
# ============================================================

st.title("🚔 Los Angeles Crime Analytics")

st.markdown(
    """
    Historical analysis of reported Los Angeles crime records.

    Use the filters to explore **where crimes occurred, which crime
    types were most common, victim demographics, and how crime volume
    changed over time**.
    """
)

st.caption(
    "Dataset: Los Angeles Police Department historical crime records, "
    "2020–2024."
)


# ============================================================
# SIDEBAR FILTERS
# ============================================================

st.sidebar.header("Dashboard Filters")


# -------------------------
# YEAR FILTER
# -------------------------

available_years = sorted(
    df["year"]
    .dropna()
    .astype(int)
    .unique()
)

min_year = min(available_years)
max_year = max(available_years)


selected_year_range = st.sidebar.slider(
    "Year range",
    min_value=min_year,
    max_value=max_year,
    value=(min_year, max_year)
)


# -------------------------
# AREA FILTER
# -------------------------

available_areas = sorted(
    df["area_name"]
    .dropna()
    .unique()
)


selected_areas = st.sidebar.multiselect(
    "Area",
    options=available_areas,
    placeholder="All areas"
)


# -------------------------
# CRIME TYPE FILTER
# -------------------------

available_crimes = sorted(
    df["crime_type"]
    .dropna()
    .unique()
)


selected_crimes = st.sidebar.multiselect(
    "Crime type",
    options=available_crimes,
    placeholder="All crime types"
)


# -------------------------
# SEX FILTER
# -------------------------

available_sexes = sorted(
    df["victim_sex"]
    .dropna()
    .unique()
)


selected_sexes = st.sidebar.multiselect(
    "Victim sex",
    options=available_sexes,
    placeholder="All"
)


# ============================================================
# APPLY FILTERS
# ============================================================

filtered_df = df[
    df["year"].between(
        selected_year_range[0],
        selected_year_range[1]
    )
].copy()


if selected_areas:

    filtered_df = filtered_df[
        filtered_df["area_name"].isin(
            selected_areas
        )
    ]


if selected_crimes:

    filtered_df = filtered_df[
        filtered_df["crime_type"].isin(
            selected_crimes
        )
    ]


if selected_sexes:

    filtered_df = filtered_df[
        filtered_df["victim_sex"].isin(
            selected_sexes
        )
    ]


# ============================================================
# EMPTY FILTER RESULT
# ============================================================

if filtered_df.empty:

    st.warning(
        "No records match the selected filters. "
        "Try changing one or more filters."
    )

    st.stop()


# ============================================================
# KPI CALCULATIONS
# ============================================================

total_crimes = filtered_df["crime_id"].nunique()


average_age = filtered_df["victim_age"].mean()

if pd.isna(average_age):
    average_age_display = "N/A"
else:
    average_age_display = f"{average_age:.1f}"


top_area, top_area_count = top_category(
    filtered_df,
    "area_name"
)


top_crime, top_crime_count = top_category(
    filtered_df,
    "crime_type"
)


top_sex, top_sex_count = top_category(
    filtered_df,
    "victim_sex"
)


yoy_change, previous_year, latest_year = (
    calculate_yoy_change(filtered_df)
)


# ============================================================
# KPI SECTION
# ============================================================

st.subheader("Key Business Metrics")


kpi1, kpi2, kpi3 = st.columns(3)


with kpi1:

    st.metric(
        label="Total Crimes",
        value=f"{total_crimes:,}"
    )


with kpi2:

    st.metric(
        label="Average Victim Age",
        value=average_age_display
    )


with kpi3:

    st.metric(
        label="Most Common Victim Sex",
        value=top_sex if top_sex else "N/A"
    )


kpi4, kpi5, kpi6 = st.columns(3)


with kpi4:

    st.metric(
        label="Area With Most Crimes",
        value=top_area if top_area else "N/A",
        help=(
            f"{top_area_count:,} crimes"
            if top_area
            else None
        )
    )


with kpi5:

    st.metric(
        label="Most Common Crime",
        value=top_crime if top_crime else "N/A",
        help=(
            f"{top_crime_count:,} crimes"
            if top_crime
            else None
        )
    )


with kpi6:

    if yoy_change is not None:

        st.metric(
            label=f"{latest_year} vs {previous_year}",
            value=f"{yoy_change:+.1f}%",
            help="Change in crime volume between the latest two selected years."
        )

    else:

        st.metric(
            label="Year-over-Year Change",
            value="N/A"
        )


# ============================================================
# BUSINESS SUMMARY
# ============================================================

top_area_share = (
    top_area_count
    / total_crimes
    * 100
)

top_crime_share = (
    top_crime_count
    / total_crimes
    * 100
)


st.info(
    f"""
    **Business summary:**  
    The selected data contains **{total_crimes:,} crimes**.
    **{top_area}** recorded the highest number of crimes
    ({top_area_count:,}, {top_area_share:.1f}% of the selected records).
    The most common crime type was **{top_crime}**
    ({top_crime_count:,}, {top_crime_share:.1f}%).
    """
)


st.divider()


# ============================================================
# BUSINESS QUESTION 1
# CRIMES BY AREA
# ============================================================

st.header("1. Where are the most crimes reported?")


area_summary = (
    filtered_df
    .groupby("area_name")
    .agg(
        crime_count=(
            "crime_id",
            "nunique"
        )
    )
    .reset_index()
)


area_fig = px.treemap(
    area_summary,
    path=["area_name"],
    values="crime_count",
    title="Crime Distribution by LAPD Area"
)


area_fig.update_traces(
    textinfo="label+value+percent parent"
)


area_fig.update_layout(
    margin=dict(
        t=50,
        l=10,
        r=10,
        b=10
    )
)


st.plotly_chart(
    area_fig,
    use_container_width=True
)


st.caption(
    "Larger rectangles represent areas with a greater number of crimes."
)


st.divider()


# ============================================================
# BUSINESS QUESTION 2
# CRIME TYPES
# ============================================================

st.header("2. Which crime types are most common?")


top_n = st.slider(
    "Number of crime types to display",
    min_value=5,
    max_value=30,
    value=15
)


crime_summary = (
    filtered_df
    .groupby("crime_type")
    .agg(
        crime_count=(
            "crime_id",
            "nunique"
        )
    )
    .reset_index()
    .sort_values(
        "crime_count",
        ascending=False
    )
    .head(top_n)
    .sort_values(
        "crime_count",
        ascending=True
    )
)


crime_fig = px.bar(
    crime_summary,
    x="crime_count",
    y="crime_type",
    orientation="h",
    title=f"Top {top_n} Crime Types",
    labels={
        "crime_count": "Number of Crimes",
        "crime_type": "Crime Type"
    }
)


crime_fig.update_layout(
    height=max(
        450,
        top_n * 30
    ),
    yaxis_title=None
)


st.plotly_chart(
    crime_fig,
    use_container_width=True
)


st.caption(
    "The chart is limited to the top crime categories to keep it readable."
)


st.divider()


# ============================================================
# BUSINESS QUESTION 3
# VICTIM DEMOGRAPHICS
# ============================================================

st.header("3. Who are the reported victims?")


age_col, sex_col = st.columns(2)


# ------------------------------------------------------------
# AGE GRAPH
# ------------------------------------------------------------

with age_col:

    st.subheader("Victim Age")


    age_order = [
        "Under 18",
        "18-24",
        "25-34",
        "35-44",
        "45-54",
        "55-64",
        "65+",
        "Unknown"
    ]


    age_summary = (
        filtered_df
        .groupby(
            "victim_age_group",
            observed=False
        )
        .agg(
            victim_count=(
                "crime_id",
                "nunique"
            )
        )
        .reset_index()
    )


    age_summary["victim_age_group"] = (
        pd.Categorical(
            age_summary["victim_age_group"],
            categories=age_order,
            ordered=True
        )
    )


    age_summary = age_summary.sort_values(
        "victim_age_group"
    )


    age_fig = px.bar(
        age_summary,
        x="victim_age_group",
        y="victim_count",
        labels={
            "victim_age_group": "Age Group",
            "victim_count": "Number of Victims"
        },
        title="Victims by Age Group"
    )


    st.plotly_chart(
        age_fig,
        use_container_width=True
    )


    valid_ages = filtered_df[
        "victim_age"
    ].dropna()


    if not valid_ages.empty:

        median_age = valid_ages.median()

        st.metric(
            "Median Victim Age",
            f"{median_age:.0f}"
        )


# ------------------------------------------------------------
# SEX GRAPH
# ------------------------------------------------------------

with sex_col:

    st.subheader("Victim Sex")


    sex_summary = (
        filtered_df
        .groupby("victim_sex")
        .agg(
            victim_count=(
                "crime_id",
                "nunique"
            )
        )
        .reset_index()
    )


    sex_fig = px.pie(
        sex_summary,
        names="victim_sex",
        values="victim_count",
        hole=0.45,
        title="Victims by Sex"
    )


    sex_fig.update_traces(
        textposition="inside",
        textinfo="percent+label"
    )


    st.plotly_chart(
        sex_fig,
        use_container_width=True
    )


    sex_total = sex_summary[
        "victim_count"
    ].sum()


    top_sex_percentage = (
        top_sex_count
        / sex_total
        * 100
    )


    st.metric(
        "Largest Victim Sex Group",
        top_sex,
        help=f"{top_sex_percentage:.1f}% of selected crimes"
    )


st.divider()


# ============================================================
# BUSINESS QUESTION 4
# CRIME TREND
# ============================================================

st.header("4. How has crime volume changed over time?")


trend_level = st.radio(
    "Time aggregation",
    options=[
        "Monthly",
        "Yearly"
    ],
    horizontal=True
)


# ------------------------------------------------------------
# MONTHLY TREND
# ------------------------------------------------------------

if trend_level == "Monthly":

    trend_df = filtered_df.dropna(
        subset=["date_occurred"]
    ).copy()


    trend_df["period"] = (
        trend_df["date_occurred"]
        .dt.to_period("M")
        .dt.to_timestamp()
    )


    trend_summary = (
        trend_df
        .groupby("period")
        .agg(
            crime_count=(
                "crime_id",
                "nunique"
            )
        )
        .reset_index()
        .sort_values("period")
    )


    trend_fig = px.line(
        trend_summary,
        x="period",
        y="crime_count",
        markers=True,
        title="Monthly Crime Trend",
        labels={
            "period": "Month",
            "crime_count": "Number of Crimes"
        }
    )


# ------------------------------------------------------------
# YEARLY TREND
# ------------------------------------------------------------

else:

    trend_summary = (
        filtered_df
        .dropna(subset=["year"])
        .groupby("year")
        .agg(
            crime_count=(
                "crime_id",
                "nunique"
            )
        )
        .reset_index()
        .sort_values("year")
    )


    trend_fig = px.line(
        trend_summary,
        x="year",
        y="crime_count",
        markers=True,
        title="Yearly Crime Trend",
        labels={
            "year": "Year",
            "crime_count": "Number of Crimes"
        }
    )


st.plotly_chart(
    trend_fig,
    use_container_width=True
)


st.caption(
    "Crime trends use the date the crime occurred rather than "
    "the date the crime was reported."
)


st.divider()


# ============================================================
# DATA QUALITY SECTION
# ============================================================

st.header("Data Quality & Coverage")


if quality_report is None:

    st.warning(
        "No data quality report was found."
    )

else:

    dq1, dq2, dq3, dq4 = st.columns(4)


    with dq1:

        st.metric(
            "Raw Records",
            f"{quality_report.get('raw_row_count', 0):,}"
        )


    with dq2:

        st.metric(
            "Duplicate Crime IDs",
            f"{quality_report.get('duplicate_crime_ids', 0):,}"
        )


    with dq3:

        st.metric(
            "Invalid Victim Ages",
            f"{quality_report.get('invalid_victim_age', 0):,}"
        )


    with dq4:

        st.metric(
            "Missing Coordinates",
            f"{quality_report.get('zero_coordinates', 0):,}"
        )


    with st.expander(
        "View detailed data quality results"
    ):

        st.write(
            "Unexpected victim sex values:",
            quality_report.get(
                "unexpected_victim_sex",
                0
            )
        )

        st.write(
            "Invalid occurrence times:",
            quality_report.get(
                "invalid_occurrence_time",
                0
            )
        )

        st.write(
            "Invalid coordinates:",
            quality_report.get(
                "invalid_coordinates",
                0
            )
        )

        st.write(
            "Number of unique LAPD areas:",
            quality_report.get(
                "unique_areas",
                0
            )
        )

        relationship_issues = (
            quality_report.get(
                "code_description_relationships",
                {}
            )
        )

        st.write(
            "Code-to-description mapping issues:",
            relationship_issues
        )


# ============================================================
# DATA EXPLORATION
# ============================================================

with st.expander(
    "Explore filtered crime records"
):

    display_columns = [
        "crime_id",
        "date_occurred",
        "area_name",
        "crime_type",
        "victim_age",
        "victim_sex",
        "premise_description",
        "weapon_description"
    ]


    available_display_columns = [
        column
        for column in display_columns
        if column in filtered_df.columns
    ]


    st.dataframe(
        filtered_df[
            available_display_columns
        ].head(1000),
        use_container_width=True
    )


# ============================================================
# FOOTER
# ============================================================

st.caption(
    "Historical crime records may contain missing, unknown, or "
    "incorrectly reported values. Data quality findings are surfaced "
    "above and documented as part of the ETL process."
)