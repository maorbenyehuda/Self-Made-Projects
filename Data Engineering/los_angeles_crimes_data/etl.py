from pathlib import Path
from datetime import datetime, timezone
import json
import logging
import time

import pandas as pd
import requests


# ============================================================
# CONFIGURATION
# ============================================================

API_URL = "https://data.lacity.org/api/v3/views/2nrs-mtv8/query.json"

API_PARAMS = {
    "accessType": "DOWNLOAD"
}

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

RAW_FILE = RAW_DIR / "crime_data_raw.json"


DROP_COLUMNS = [
    ":id",
    ":version",
    ":created_at",
    ":updated_at",
    "part_1_2",
    "cross_street",
    "crm_cd_2",
    "crm_cd_3",
    "crm_cd_4",
    "crm_cd_1",
    "mocodes",
    "crm_cd",
    "rpt_dist_no",
    "area",
    "premis_cd",
    "weapon_used_cd",
    "status",
]


REQUIRED_COLUMNS = [
    "dr_no",
    "date_rptd",
    "date_occ",
    "time_occ",
    "area",
    "area_name",
    "crm_cd",
    "crm_cd_desc",
    "vict_age",
    "vict_sex",
    "premis_cd",
    "premis_desc",
    "status",
    "status_desc",
    "lat",
    "lon",
]


RENAME_COLUMNS = {
    "dr_no": "crime_id",
    "date_rptd": "date_reported",
    "date_occ": "date_occurred",
    "time_occ": "time_occurred",
    "area_name": "area_name",
    "crm_cd_desc": "crime_type",
    "vict_age": "victim_age",
    "vict_sex": "victim_sex",
    "vict_descent": "victim_descent",
    "premis_desc": "premise_description",
    "status_desc": "status_description",
    "weapon_desc": "weapon_description",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# SETUP
# ============================================================

def create_directories():
    """Create project data directories if they do not exist."""

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# EXTRACTION
# ============================================================

def extract_data(max_retries=3):
    """
    Extract crime data from the Los Angeles Open Data API.

    Includes:
    - HTTP validation
    - timeout handling
    - retries
    - raw data persistence
    """

    logger.info("Starting API extraction...")

    last_error = None

    for attempt in range(1, max_retries + 1):

        try:
            logger.info(
                "API request attempt %s/%s",
                attempt,
                max_retries
            )

            response = requests.get(
                API_URL,
                params=API_PARAMS,
                timeout=(10, 180)
            )

            response.raise_for_status()

            data = response.json()

            if not data:
                raise ValueError("API returned an empty response.")

            # Normally the API returns a list of records.
            # This additionally protects us if the response structure changes.
            if isinstance(data, list):
                records = data

            elif isinstance(data, dict) and isinstance(data.get("data"), list):
                records = data["data"]

            else:
                raise ValueError(
                    f"Unexpected API response structure: {type(data)}"
                )

            with open(RAW_FILE, "w", encoding="utf-8") as file:
                json.dump(data, file, ensure_ascii=False, indent=2)

            df = pd.DataFrame(records)

            logger.info(
                "Extraction completed successfully: %s rows, %s columns",
                len(df),
                len(df.columns)
            )

            return df

        except (
            requests.RequestException,
            ValueError,
            json.JSONDecodeError
        ) as exc:

            last_error = exc

            logger.warning(
                "Extraction attempt %s failed: %s",
                attempt,
                exc
            )

            if attempt < max_retries:
                time.sleep(2 * attempt)

    raise RuntimeError(
        f"API extraction failed after {max_retries} attempts."
    ) from last_error


# ============================================================
# SCHEMA VALIDATION
# ============================================================

def validate_schema(df):
    """Ensure required columns are available before transformation."""

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Required columns missing from API response: "
            f"{missing_columns}"
        )

    if df.empty:
        raise ValueError("Dataset contains no rows.")

    logger.info("Schema validation passed.")


# ============================================================
# DATA QUALITY
# ============================================================

def check_code_description_relationship(
    df,
    code_column,
    description_column
):
    """
    Check whether one code maps to multiple descriptions.
    """

    relationship = (
        df[[code_column, description_column]]
        .dropna()
        .groupby(code_column)[description_column]
        .nunique()
    )

    return int((relationship > 1).sum())


def generate_raw_quality_metrics(df):

    logger.info("Running raw data quality checks...")

    age = pd.to_numeric(
        df["vict_age"],
        errors="coerce"
    )

    time_occ = pd.to_numeric(
        df["time_occ"],
        errors="coerce"
    )

    hours = time_occ // 100
    minutes = time_occ % 100

    invalid_time = (
        time_occ.notna()
        &
        (
            ~hours.between(0, 23)
            |
            ~minutes.between(0, 59)
        )
    )

    sex = (
        df["vict_sex"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    unexpected_sex = (
        sex.notna()
        & ~sex.isin(["M", "F", "X"])
    )

    lat = pd.to_numeric(df["lat"], errors="coerce")
    lon = pd.to_numeric(df["lon"], errors="coerce")

    zero_coordinates = (
        (lat == 0)
        &
        (lon == 0)
    )

    invalid_coordinates = (
        (lat.notna() & ~lat.between(-90, 90))
        |
        (lon.notna() & ~lon.between(-180, 180))
    )

    date_occurred = pd.to_datetime(
        df["date_occ"],
        errors="coerce"
    )

    date_reported = pd.to_datetime(
        df["date_rptd"],
        errors="coerce"
    )

    report_before_occurrence = (
        date_reported.notna()
        &
        date_occurred.notna()
        &
        (date_reported < date_occurred)
    )

    quality = {
        "raw_row_count": int(len(df)),

        "raw_column_count": int(len(df.columns)),

        "duplicate_crime_ids": int(
            df["dr_no"].duplicated().sum()
        ),

        "missing_crime_ids": int(
            df["dr_no"].isna().sum()
        ),

        "missing_area_name": int(
            df["area_name"].isna().sum()
        ),

        "missing_crime_type": int(
            df["crm_cd_desc"].isna().sum()
        ),

        "missing_occurrence_date": int(
            df["date_occ"].isna().sum()
        ),

        "invalid_occurrence_dates": int(
            date_occurred.isna().sum()
        ),

        "invalid_report_dates": int(
            date_reported.isna().sum()
        ),

        "report_date_before_occurrence_date": int(
            report_before_occurrence.sum()
        ),

        "invalid_victim_age": int(
            (
                age.notna()
                &
                ~age.between(1, 99)
            ).sum()
        ),

        "unexpected_victim_sex": int(
            unexpected_sex.sum()
        ),

        "invalid_occurrence_time": int(
            invalid_time.sum()
        ),

        "zero_coordinates": int(
            zero_coordinates.sum()
        ),

        "invalid_coordinates": int(
            invalid_coordinates.sum()
        ),

        "unique_areas": int(
            df["area_name"].nunique()
        ),

        "expected_lapd_areas": 21,

        "code_description_relationships": {

            "area": check_code_description_relationship(
                df,
                "area",
                "area_name"
            ),

            "crime": check_code_description_relationship(
                df,
                "crm_cd",
                "crm_cd_desc"
            ),

            "premise": check_code_description_relationship(
                df,
                "premis_cd",
                "premis_desc"
            ),

            "status": check_code_description_relationship(
                df,
                "status",
                "status_desc"
            ),
        }
    }

    return quality


# ============================================================
# TRANSFORMATION
# ============================================================

def clean_text_columns(df, columns):

    for column in columns:

        if column in df.columns:

            df[column] = (
                df[column]
                .astype("string")
                .str.strip()
            )

    return df


def transform_data(df):

    logger.info("Starting transformations...")

    df = df.copy()

    # --------------------------------------------------------
    # Remove duplicate versions of the same crime report
    # --------------------------------------------------------

    if ":version" in df.columns:

        df["_version_sort"] = pd.to_numeric(
            df[":version"],
            errors="coerce"
        )

        df = (
            df.sort_values(
                ["dr_no", "_version_sort"],
                na_position="first"
            )
            .drop_duplicates(
                subset="dr_no",
                keep="last"
            )
        )

        df.drop(
            columns="_version_sort",
            inplace=True
        )

    else:

        df = df.drop_duplicates(
            subset="dr_no",
            keep="last"
        )

    # --------------------------------------------------------
    # Dates
    # --------------------------------------------------------

    df["date_occ"] = pd.to_datetime(
        df["date_occ"],
        errors="coerce"
    )

    df["date_rptd"] = pd.to_datetime(
        df["date_rptd"],
        errors="coerce"
    )

    # --------------------------------------------------------
    # Time
    # --------------------------------------------------------

    time_numeric = pd.to_numeric(
        df["time_occ"],
        errors="coerce"
    )

    hour = time_numeric // 100
    minute = time_numeric % 100

    valid_time = (
        hour.between(0, 23)
        &
        minute.between(0, 59)
    )

    df["occurrence_hour"] = hour.where(
        valid_time,
        pd.NA
    ).astype("Int64")

    # --------------------------------------------------------
    # Victim age
    # --------------------------------------------------------

    df["vict_age"] = pd.to_numeric(
        df["vict_age"],
        errors="coerce"
    )

    valid_age = df["vict_age"].between(
        1,
        99
    )

    df.loc[
        ~valid_age,
        "vict_age"
    ] = pd.NA

    # --------------------------------------------------------
    # Victim sex
    # --------------------------------------------------------

    sex_mapping = {
        "M": "Male",
        "F": "Female",
        "X": "Unknown"
    }

    sex = (
        df["vict_sex"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    df["vict_sex"] = (
        sex
        .map(sex_mapping)
        .fillna("Unknown")
    )

    # --------------------------------------------------------
    # Coordinates
    # --------------------------------------------------------

    df["lat"] = pd.to_numeric(
        df["lat"],
        errors="coerce"
    )

    df["lon"] = pd.to_numeric(
        df["lon"],
        errors="coerce"
    )

    missing_coordinates = (
        (df["lat"] == 0)
        &
        (df["lon"] == 0)
    )

    df.loc[
        missing_coordinates,
        ["lat", "lon"]
    ] = pd.NA

    # --------------------------------------------------------
    # Clean text
    # --------------------------------------------------------

    text_columns = [
        "area_name",
        "crm_cd_desc",
        "vict_descent",
        "premis_desc",
        "status_desc",
        "weapon_desc",
        "location",
    ]

    df = clean_text_columns(
        df,
        text_columns
    )

    # --------------------------------------------------------
    # Drop unnecessary technical/code columns
    # --------------------------------------------------------

    columns_to_drop = [
        column
        for column in DROP_COLUMNS
        if column in df.columns
    ]

    df.drop(
        columns=columns_to_drop,
        inplace=True
    )

    # --------------------------------------------------------
    # Rename columns
    # --------------------------------------------------------

    df.rename(
        columns=RENAME_COLUMNS,
        inplace=True
    )

    # --------------------------------------------------------
    # Analytical date dimensions
    # --------------------------------------------------------

    df["year"] = (
        df["date_occurred"]
        .dt.year
        .astype("Int64")
    )

    df["month"] = (
        df["date_occurred"]
        .dt.month
        .astype("Int64")
    )

    df["month_name"] = (
        df["date_occurred"]
        .dt.month_name()
    )

    df["year_month"] = (
        df["date_occurred"]
        .dt.to_period("M")
        .astype("string")
    )

    # --------------------------------------------------------
    # Victim age groups
    # --------------------------------------------------------

    age_bins = [
        0,
        17,
        24,
        34,
        44,
        54,
        64,
        float("inf")
    ]

    age_labels = [
        "Under 18",
        "18-24",
        "25-34",
        "35-44",
        "45-54",
        "55-64",
        "65+"
    ]

    df["victim_age_group"] = pd.cut(
        df["victim_age"],
        bins=age_bins,
        labels=age_labels
    )

    df["victim_age_group"] = (
        df["victim_age_group"]
        .astype("string")
        .fillna("Unknown")
    )

    logger.info(
        "Transformations completed: %s rows",
        len(df)
    )

    return df


# ============================================================
# ANALYTICAL OUTPUTS
# ============================================================

def create_analytical_outputs(df):

    logger.info("Creating analytical outputs...")

    # --------------------------------------------------------
    # Crimes by Area
    # --------------------------------------------------------

    crimes_by_area = (
        df.groupby(
            "area_name",
            dropna=False
        )
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
    )

    # --------------------------------------------------------
    # Crimes by Type
    # --------------------------------------------------------

    crimes_by_type = (
        df.groupby(
            "crime_type",
            dropna=False
        )
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
    )

    # --------------------------------------------------------
    # Victim Age Distribution
    # --------------------------------------------------------

    victim_age_distribution = (
        df.dropna(
            subset=["victim_age"]
        )
        .groupby("victim_age")
        .size()
        .reset_index(
            name="victim_count"
        )
        .sort_values("victim_age")
    )

    # --------------------------------------------------------
    # Victim Age Groups
    # --------------------------------------------------------

    victim_age_groups = (
        df.groupby(
            "victim_age_group",
            dropna=False
        )
        .size()
        .reset_index(
            name="victim_count"
        )
    )

    # --------------------------------------------------------
    # Victim Sex
    # --------------------------------------------------------

    victim_sex = (
        df.groupby(
            "victim_sex",
            dropna=False
        )
        .size()
        .reset_index(
            name="victim_count"
        )
    )

    victim_sex["percentage"] = (
        victim_sex["victim_count"]
        /
        victim_sex["victim_count"].sum()
        *
        100
    ).round(2)

    # --------------------------------------------------------
    # Monthly Crime Trend
    # --------------------------------------------------------

    monthly = (
        df.dropna(
            subset=["date_occurred"]
        )
        .groupby(
            ["year", "month", "year_month"]
        )
        .agg(
            crime_count=(
                "crime_id",
                "nunique"
            )
        )
        .reset_index()
        .sort_values(
            ["year", "month"]
        )
    )

    monthly["month_start"] = pd.to_datetime(
        monthly["year_month"] + "-01"
    )

    # --------------------------------------------------------
    # Yearly Crime Trend
    # --------------------------------------------------------

    yearly = (
        df.dropna(
            subset=["year"]
        )
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

    return {
        "crimes_by_area": crimes_by_area,
        "crimes_by_type": crimes_by_type,
        "victim_age_distribution": victim_age_distribution,
        "victim_age_groups": victim_age_groups,
        "victim_sex_distribution": victim_sex,
        "crime_monthly_trend": monthly,
        "crime_yearly_trend": yearly,
    }


# ============================================================
# KPI LAYER
# ============================================================

def create_kpi_summary(df):

    valid_age = df["victim_age"].dropna()

    area_counts = (
        df.groupby("area_name")["crime_id"]
        .nunique()
        .sort_values(ascending=False)
    )

    crime_counts = (
        df.groupby("crime_type")["crime_id"]
        .nunique()
        .sort_values(ascending=False)
    )

    sex_counts = (
        df["victim_sex"]
        .value_counts()
    )

    kpis = {
        "total_crimes": int(
            df["crime_id"].nunique()
        ),

        "average_victim_age": (
            round(float(valid_age.mean()), 1)
            if not valid_age.empty
            else None
        ),

        "median_victim_age": (
            round(float(valid_age.median()), 1)
            if not valid_age.empty
            else None
        ),

        "most_affected_area": (
            str(area_counts.index[0])
            if not area_counts.empty
            else None
        ),

        "most_affected_area_crimes": (
            int(area_counts.iloc[0])
            if not area_counts.empty
            else None
        ),

        "most_common_crime_type": (
            str(crime_counts.index[0])
            if not crime_counts.empty
            else None
        ),

        "most_common_crime_count": (
            int(crime_counts.iloc[0])
            if not crime_counts.empty
            else None
        ),

        "most_common_victim_sex": (
            str(sex_counts.index[0])
            if not sex_counts.empty
            else None
        ),

        "first_year": (
            int(df["year"].min())
            if df["year"].notna().any()
            else None
        ),

        "last_year": (
            int(df["year"].max())
            if df["year"].notna().any()
            else None
        ),
    }

    return kpis


# ============================================================
# LOAD
# ============================================================

def save_outputs(
    df,
    analytical_outputs,
    kpis,
    quality_report
):

    logger.info("Saving processed outputs...")

    # Clean analytical dataset
    df.to_csv(
        PROCESSED_DIR / "crime_clean.csv",
        index=False
    )

    # Aggregated datasets
    for name, dataframe in analytical_outputs.items():

        dataframe.to_csv(
            PROCESSED_DIR / f"{name}.csv",
            index=False
        )

    # KPI summary
    with open(
        PROCESSED_DIR / "kpi_summary.json",
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            kpis,
            file,
            indent=4
        )

    # Data-quality report
    with open(
        PROCESSED_DIR / "data_quality_report.json",
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            quality_report,
            file,
            indent=4
        )

    logger.info("All outputs saved successfully.")


# ============================================================
# MAIN ETL PIPELINE
# ============================================================

def main():

    logger.info("=" * 60)
    logger.info("LA Crime Analytics ETL")
    logger.info("=" * 60)

    create_directories()

    # EXTRACT
    raw_df = extract_data()

    # VALIDATE
    validate_schema(raw_df)

    quality_report = generate_raw_quality_metrics(
        raw_df
    )

    # TRANSFORM
    clean_df = transform_data(
        raw_df
    )

    quality_report["processed_row_count"] = int(
        len(clean_df)
    )

    quality_report["extraction_timestamp_utc"] = (
        datetime.now(timezone.utc).isoformat()
    )

    # ANALYTICS
    analytical_outputs = create_analytical_outputs(
        clean_df
    )

    kpis = create_kpi_summary(
        clean_df
    )

    # LOAD
    save_outputs(
        clean_df,
        analytical_outputs,
        kpis,
        quality_report
    )

    logger.info("=" * 60)
    logger.info("ETL FINISHED SUCCESSFULLY")
    logger.info("Total crimes: %s", kpis["total_crimes"])
    logger.info("=" * 60)


if __name__ == "__main__":
    main()