# Los Angeles Crime Analytics

An end-to-end data engineering and analytics project built with **Python, Pandas, Requests, Plotly, and Streamlit**.

The project extracts public crime data from the City of Los Angeles Open Data API, stores the original API response locally, validates and transforms the data, creates analytical outputs, and presents the results through an interactive business-facing Streamlit dashboard.

The goal of the project is not only to visualize crime data, but to demonstrate a complete and reproducible data workflow:

```text
Public API
    ↓
Extraction
    ↓
Raw Data Storage
    ↓
Schema & Data Quality Validation
    ↓
Transformation / Cleaning
    ↓
Analytical Data Layer
    ↓
Processed Local Files
    ↓
Streamlit Dashboard
```

---

# 1. Project Objective

The objective of this project is to build an end-to-end data product that allows a business user or analyst to explore historical Los Angeles crime patterns.

The application focuses on four main analytical questions:

1. **Which Los Angeles areas report the highest number of crimes?**
2. **Which crime types occur most frequently?**
3. **What are the main victim demographic patterns by age and sex?**
4. **How has crime volume changed over months and years?**

In addition to answering these questions, the project includes:

* API error handling
* raw data persistence
* schema validation
* duplicate detection
* invalid-value detection
* code-to-description relationship validation
* analytical aggregations
* KPI calculations
* interactive filtering
* data-quality reporting

---

# 2. Data Source

The project uses the **Los Angeles Police Department Crime Data from 2020 to 2024** dataset.

The dataset was discovered through:

* Public APIs directory
* Data.gov
* City of Los Angeles Open Data

The actual data extraction is performed directly from the City of Los Angeles public API.

API endpoint:

```text
https://data.lacity.org/api/v3/views/2nrs-mtv8/query.json
```

The API does not require:

* paid access
* private credentials
* reviewer-provided secrets
* manual file downloads

This makes it suitable for a reproducible home-assignment environment.

The extraction request uses:

```python
params = {
    "accessType": "DOWNLOAD"
}
```

---

# 3. Why This Dataset Was Selected

I selected this dataset because it provides enough depth to demonstrate both **data engineering** and **business analytics** skills.

The dataset contains:

* crime identifiers
* occurrence dates
* report dates
* geographic LAPD areas
* crime descriptions
* victim demographics
* premise information
* weapon information
* crime status
* geographic coordinates

This provides several dimensions that can be transformed into meaningful analytical outputs without requiring external databases or additional paid services.

The dataset is also large enough to make ETL design, validation, data-quality checks, and aggregation meaningful.

---

# 4. Project Structure

```text
AI Data Engineer Assignment/
│
├── app.py
│
├── etl.py
│
├── README.md
│
├── requirements.txt
│
├── .gitignore
│
│
├── data/
│   │
│   ├── raw/
│   │   └── crime_data_raw.json
│   │
│   └── processed/
│       ├── crime_clean.csv
│       ├── crimes_by_area.csv
│       ├── crimes_by_type.csv
│       ├── victim_age_distribution.csv
│       ├── victim_age_groups.csv
│       ├── victim_sex_distribution.csv
│       ├── crime_monthly_trend.csv
│       ├── crime_yearly_trend.csv
│       ├── kpi_summary.json
│       └── data_quality_report.json
│
├── ai_transcript/
│   └── ...
│
└── tests/
    └── ...
```

The `tests/` directory is optional and may be expanded in a future version.

---

# 5. Architecture

The project separates the data engineering process from the presentation layer.

The Streamlit application does **not** retrieve or clean the API data directly.

Instead:

```text
                Los Angeles Open Data API
                          │
                          ▼
                     Extraction
                          │
                          ▼
               data/raw/crime_data_raw.json
                          │
                          ▼
               Schema & Quality Validation
                          │
                          ▼
                    Transformation
                          │
                          ▼
               Clean Analytical Dataset
                          │
              ┌───────────┼────────────┐
              │           │            │
              ▼           ▼            ▼
          Area Data   Crime Data   Victim Data
              │           │            │
              └───────────┼────────────┘
                          │
                          ▼
                  Processed Outputs
                          │
                          ▼
                     Streamlit
```

This design was chosen deliberately.

Separating ETL from Streamlit provides several benefits:

* the API is not called every time the dashboard reloads
* transformations have one defined location
* data-quality checks are performed before visualization
* the raw source remains reproducible
* analytical outputs can be inspected independently
* dashboard code remains focused on presentation and interaction

---

# 6. Running the Project

The project is designed to follow the standard assignment run flow.

## Clone the repository

```bash
git clone <repository-url>
cd <repository-folder>
```

## Create a virtual environment

```bash
python -m venv .venv
```

### Windows

```bash
.venv\Scripts\activate
```

### macOS / Linux

```bash
source .venv/bin/activate
```

## Install dependencies

```bash
pip install -r requirements.txt
```

## Run the ETL pipeline

```bash
python etl.py
```

This performs:

```text
API extraction
→ raw persistence
→ schema validation
→ data-quality validation
→ transformation
→ analytical aggregation
→ processed output creation
```

## Run the Streamlit application

```bash
streamlit run app.py
```

Streamlit will provide a local browser address, typically:

```text
http://localhost:8501
```

---

# 7. Python Dependencies

The main project dependencies are:

```text
pandas
requests
streamlit
plotly
```

The final tested dependency versions are listed in `requirements.txt`.

---

# 8. Extraction Layer

The extraction logic is implemented in `etl.py`.

The API is accessed using Python's `requests` library.

Example request:

```python
response = requests.get(
    API_URL,
    params=API_PARAMS,
    timeout=(10, 180)
)

response.raise_for_status()
```

Several reliability mechanisms are included.

## HTTP validation

```python
response.raise_for_status()
```

ensures HTTP failures such as:

```text
404
500
503
```

are treated as failures rather than silently processed.

## Timeout handling

The API call uses an explicit timeout.

This prevents the ETL process from waiting indefinitely if the external service becomes unavailable.

## Retry logic

Extraction attempts are retried when temporary failures occur.

The retry mechanism provides additional resilience against short-lived network or API problems.

## Response validation

The returned JSON structure is checked before it is converted into a Pandas DataFrame.

If the API returns:

* an empty response
* an unexpected structure
* malformed JSON

the ETL pipeline stops with a meaningful error instead of generating corrupted processed files.

---

# 9. Raw Data Layer

The raw API response is stored in:

```text
data/raw/crime_data_raw.json
```

The raw file is intentionally stored before transformation.

This decision supports:

* reproducibility
* debugging
* source inspection
* comparison between raw and processed data
* future reprocessing without necessarily redesigning the extraction logic

The file is kept in JSON format because the API itself returns JSON.

A plain JSON file was preferred over compressed JSON for this project because it is easier for a reviewer to inspect directly.

The assignment prioritizes clarity and practical judgment, so making the raw source human-readable was considered more valuable than minimizing local disk usage.

---

# 10. Schema Validation

Before transformation begins, the ETL verifies that the expected fields still exist.

Examples include:

```text
dr_no
date_rptd
date_occ
time_occ
area
area_name
crm_cd
crm_cd_desc
vict_age
vict_sex
premis_cd
premis_desc
status
status_desc
lat
lon
```

If an important field disappears or the external API changes its schema, the pipeline raises an error.

This prevents later transformations from silently producing incorrect outputs.

---

# 11. Column Selection

The original dataset contains several technical fields and coded fields that are unnecessary for the final analytical layer.

Examples removed include:

```text
:id
:version
:created_at
:updated_at
part_1_2
cross_street
crm_cd_1
crm_cd_2
crm_cd_3
crm_cd_4
mocodes
rpt_dist_no
```

Several numeric code fields are also removed after validation:

```text
area
crm_cd
premis_cd
weapon_used_cd
status
```

Their readable descriptive counterparts are retained:

```text
area_name
crm_cd_desc
premis_desc
weapon_desc
status_desc
```

For example:

```text
area → area_name
crm_cd → crm_cd_desc
premis_cd → premis_desc
weapon_used_cd → weapon_desc
status → status_desc
```

---

# 12. Why Code Columns Are Not Removed Immediately

Although the numerical code fields are unnecessary for the dashboard, they still provide value during data validation.

Before removing them, the ETL verifies relationships such as:

```text
area → area_name
crm_cd → crm_cd_desc
premis_cd → premis_desc
status → status_desc
```

The goal is to detect situations where the same code unexpectedly maps to multiple descriptions.

For example:

```text
Area code 1 → Central
Area code 1 → Another Area
```

would indicate an unexpected relationship.

Only after this validation is completed are the unnecessary code fields removed.

This preserves useful information during the validation stage while keeping the final analytical dataset clean.

---

# 13. Crime Identifier

The original field:

```text
dr_no
```

is retained and renamed:

```text
crime_id
```

This field is important even though it is not a primary dashboard dimension.

It allows the project to:

* detect duplicate crime records
* calculate distinct crime counts
* avoid assuming that every DataFrame row automatically represents a unique incident

Most crime calculations therefore use:

```python
df["crime_id"].nunique()
```

instead of simply:

```python
len(df)
```

---

# 14. Date Transformation

Two important dates exist in the source data:

```text
date_rptd
date_occ
```

They represent different concepts.

`date_rptd` represents when a crime was reported.

`date_occ` represents when the crime occurred.

For crime trend analysis, this project primarily uses:

```text
date_occ
```

because the business question is about when crime activity occurred rather than when reports were entered.

The fields are renamed:

```text
date_rptd → date_reported
date_occ  → date_occurred
```

Additional analytical dimensions are derived:

```text
year
month
month_name
year_month
```

These make monthly and yearly aggregation straightforward.

---

# 15. Time Transformation

The source contains:

```text
time_occ
```

in 24-hour military-style numeric format.

The ETL separates the hour component and validates that:

```text
hour   ∈ 0–23
minute ∈ 0–59
```

Invalid time values are identified as data-quality issues.

The processed dataset includes:

```text
occurrence_hour
```

which could later support additional analysis such as crimes by time of day.

---

# 16. Victim Age Transformation

Victim age is converted into a numeric field.

Values outside the accepted range are considered invalid and are converted to missing analytical values.

The cleaned field is:

```text
victim_age
```

Age groups are also created:

```text
Under 18
18-24
25-34
35-44
45-54
55-64
65+
Unknown
```

This transformation was introduced because individual ages are useful for statistics, but grouped ages are easier to understand in a business-facing dashboard.

The exact age remains available for metrics such as:

* average victim age
* median victim age

while age groups support visual comparison.

---

# 17. Victim Sex Transformation

The original dataset represents victim sex using abbreviated values.

These are transformed into readable business-facing categories.

For example:

```text
M → Male
F → Female
X → Unknown
```

Missing or unexpected values are also treated as:

```text
Unknown
```

The goal is to avoid exposing implementation-specific codes to dashboard users.

---

# 18. Coordinate Validation

Latitude and longitude are converted to numeric values.

The ETL checks for:

* latitude outside valid geographic bounds
* longitude outside valid geographic bounds
* missing coordinates
* `(0, 0)` coordinate pairs

Coordinates of:

```text
0, 0
```

are treated as unavailable rather than as an actual crime location.

This is important because leaving those coordinates untouched could produce misleading geographic analysis in future dashboard extensions.

---

# 19. Duplicate Handling

Duplicate crime identifiers are checked before analytical outputs are created.

When multiple versions of the same crime record exist, the ETL attempts to retain the latest available version when version metadata is available.

The purpose is to avoid counting one crime multiple times simply because the source dataset contains multiple versions of the same record.

Duplicate counts are also surfaced in the data-quality report.

---

# 20. Data Quality Checks

Data quality is a dedicated part of the ETL pipeline rather than an afterthought.

The project evaluates issues including:

### Missing identifiers

```text
Missing crime IDs
```

### Duplicate identifiers

```text
Duplicate crime IDs
```

### Missing business dimensions

Examples:

```text
Missing area names
Missing crime descriptions
Missing occurrence dates
```

### Invalid dates

Dates that cannot be converted into valid datetime values are reported.

### Reporting chronology

The ETL checks cases where:

```text
date_reported < date_occurred
```

These records are surfaced for review rather than silently ignored.

### Victim age validation

Invalid or unrealistic age values are identified.

### Victim sex validation

Unexpected source categories are detected.

### Time validation

Occurrence time must represent a valid 24-hour time.

### Geographic validation

Latitude and longitude must fall within valid coordinate ranges.

### Missing geographic coordinates

Records using `(0, 0)` are counted separately.

### LAPD area coverage

The number of unique areas is checked and reported.

### Code-to-description relationships

The ETL validates relationships including:

```text
area → area_name
crime code → crime description
premise code → premise description
status code → status description
```

---

# 21. Data Quality Report

Data-quality findings are persisted in:

```text
data/processed/data_quality_report.json
```

Example structure:

```json
{
    "raw_row_count": 0,
    "duplicate_crime_ids": 0,
    "missing_area_name": 0,
    "invalid_victim_age": 0,
    "unexpected_victim_sex": 0,
    "invalid_occurrence_time": 0,
    "zero_coordinates": 0,
    "invalid_coordinates": 0
}
```

The actual values are generated dynamically when `etl.py` runs.

The report is also surfaced inside the Streamlit application.

This allows a reviewer to understand not only the analytical results but also the reliability and limitations of the underlying source.

---

# 22. Processed Analytical Dataset

The primary cleaned dataset is stored in:

```text
data/processed/crime_clean.csv
```

This file represents the central analytical dataset consumed by Streamlit.

Unlike the raw layer, compression is appropriate here because this file is primarily machine-consumed rather than intended for direct manual inspection.

The dataset contains cleaned fields such as:

```text
crime_id
date_reported
date_occurred
area_name
crime_type
victim_age
victim_age_group
victim_sex
victim_descent
premise_description
weapon_description
status_description
location
lat
lon
year
month
month_name
year_month
occurrence_hour
```

---

# 23. Analytical Outputs

In addition to the main clean dataset, the ETL generates dedicated analytical outputs.

## Crimes by Area

```text
crimes_by_area.csv
```

Contains crime counts grouped by LAPD area.

Supports the business question:

> Which areas have the highest crime volume?

---

## Crimes by Crime Type

```text
crimes_by_type.csv
```

Contains crime counts grouped by readable crime description.

Supports:

> Which crime types occur most frequently?

---

## Victim Age Distribution

```text
victim_age_distribution.csv
```

Contains victim counts by exact age.

---

## Victim Age Groups

```text
victim_age_groups.csv
```

Contains victim counts grouped into business-friendly age ranges.

---

## Victim Sex Distribution

```text
victim_sex_distribution.csv
```

Contains victim counts and proportions by sex category.

---

## Monthly Crime Trend

```text
crime_monthly_trend.csv
```

Contains crime counts by month.

---

## Yearly Crime Trend

```text
crime_yearly_trend.csv
```

Contains crime counts by year.

---

## KPI Summary

```text
kpi_summary.json
```

Contains high-level calculated metrics such as:

* total crime count
* average victim age
* median victim age
* area with the most crimes
* most common crime type
* most common victim sex
* first year in the dataset
* last year in the dataset

---

# 24. Why Multiple Analytical Outputs Are Generated

The ETL intentionally generates more than one processed output.

The purpose is to separate:

```text
Detailed analytical data
```

from:

```text
Business-specific aggregated data
```

For example:

```text
crime_clean.csv
```

contains the detailed records, while:

```text
crimes_by_area.csv
```

contains a ready-to-use area-level aggregation.

This makes the analytical layer:

* reusable
* inspectable
* easier to validate
* independent from Streamlit

However, the Streamlit application uses the clean detailed dataset for interactive charts.

This is necessary because dashboard filters dynamically change the population being analyzed.

For example, if the user filters:

```text
Area = Hollywood
Year = 2023
Victim Sex = Female
```

a static aggregation generated before those selections would no longer represent the selected population.

Therefore the architecture is:

```text
ETL
├── clean detailed analytical dataset
├── reusable static aggregate outputs
├── KPI output
└── quality report

Streamlit
└── dynamically aggregates clean data after filters
```

---

# 25. Streamlit Dashboard

The Streamlit application is implemented in:

```text
app.py
```

It is designed as a business-facing analytical interface rather than a technical data inspection tool.

The dashboard includes:

* global filters
* business KPIs
* crime-area analysis
* crime-type analysis
* victim demographic analysis
* time-series trends
* data-quality information
* filtered record exploration

---

# 26. Dashboard Filters

The sidebar includes global filters for:

```text
Year range
Area
Crime type
Victim sex
```

These filters are applied to one common filtered dataset.

As a result, selecting a value updates all relevant:

* KPIs
* charts
* demographic summaries
* trends

This provides a consistent analytical experience.

---

# 27. Dashboard KPIs

The dashboard includes metrics such as:

```text
Total Crimes
Average Victim Age
Most Common Victim Sex
Area With Most Crimes
Most Common Crime
Year-over-Year Change
```

These KPIs allow a business stakeholder to understand the selected dataset before examining detailed charts.

---

# 28. Business Question 1 — Crime by Area

Question:

> Which Los Angeles areas report the highest number of crimes?

Visualization:

```text
Treemap
```

The size of each rectangle represents crime volume.

A treemap was selected because it communicates relative contribution across multiple geographic areas while using screen space efficiently.

---

# 29. Business Question 2 — Crime Type

Question:

> Which crime categories occur most frequently?

Visualization:

```text
Horizontal bar chart
```

A horizontal bar chart was selected instead of a vertical chart because crime descriptions can be relatively long.

Horizontal labels remain readable without requiring aggressive text rotation.

The user can also control the number of top crime types displayed.

---

# 30. Business Question 3 — Victim Demographics

Victim demographics are analyzed using both visualizations and KPI-style summaries.

## Age

Visualization:

```text
Bar chart by age group
```

Additional metrics:

```text
Average victim age
Median victim age
```

Grouped ages improve readability while exact ages remain available for calculations.

## Sex

Visualization:

```text
Donut chart
```

Additional metrics identify the largest victim sex group.

---

# 31. Business Question 4 — Crime Over Time

Question:

> How has crime volume changed over time?

Visualization:

```text
Line chart
```

The dashboard supports switching between:

```text
Monthly
Yearly
```

analysis.

Crime trends use:

```text
date_occurred
```

rather than:

```text
date_reported
```

because the purpose is to analyze when crimes occurred.

---

# 32. Dynamic Dashboard Aggregation

Although aggregated CSV files are generated during ETL, the dashboard recalculates metrics from the clean dataset after filters are applied.

Example:

```text
Full Dataset
     │
     ▼
Year = 2023
Area = Hollywood
Sex = Female
     │
     ▼
Filtered Dataset
     │
     ├── KPIs
     ├── Treemap
     ├── Crime Type Chart
     ├── Victim Charts
     └── Trend Chart
```

This approach keeps dashboard results internally consistent.

---

# 33. Dashboard Error Handling

The Streamlit application checks whether the required processed data exists.

If the user starts Streamlit before running the ETL pipeline, the application provides a readable message such as:

```text
Processed data was not found.
Run `python etl.py` before starting the dashboard.
```

instead of exposing a Python `FileNotFoundError`.

This improves the local execution experience for reviewers.

---

# 34. Caching

Streamlit data loading uses:

```python
@st.cache_data
```

This avoids repeatedly loading unchanged processed files during every dashboard interaction.

Caching improves dashboard responsiveness while preserving simple local execution.

---

# 35. Assumptions

Several assumptions were made during development.

## Crime ID

`dr_no` is treated as the identifier used to distinguish crime records.

## Occurrence date

`date_occ` is used for time-based crime analysis.

## Invalid ages

Victim ages outside the accepted analytical range are treated as unavailable rather than used in demographic metrics.

## Unknown sex

Unknown, missing, or unexpected victim-sex values are grouped under:

```text
Unknown
```

## Coordinates

`(0, 0)` coordinates are treated as missing geographic information.

## Code columns

Readable description fields are preferred over internal numeric codes in the final analytical layer.

---

# 36. Known Limitations

## Historical dataset

The selected dataset contains historical LAPD crime records from the legacy reporting dataset and should not be interpreted as a real-time crime monitoring system.

The dashboard is therefore presented as:

```text
Los Angeles Crime Analytics — Historical Data
```

rather than a current live crime dashboard.

## Source-system transition

The source dataset covers a period in which LAPD reporting systems changed.

As a result, later periods may not be directly comparable with earlier complete calendar years.

This is particularly important when interpreting the final year in the dataset.

## Source-data quality

The project cannot correct unknown inaccuracies in the original police records.

Instead, it detects and surfaces measurable issues where possible.

## Missing victim information

Not every crime record contains complete victim demographic information.

Therefore demographic analysis should not automatically be interpreted as representing every reported crime equally.

## Missing coordinates

Some records do not contain usable geographic coordinates.

## No external enrichment

This version does not enrich crime data with external information such as:

* population
* socioeconomic indicators
* weather
* area population density
* unemployment
* census demographics

As a result, the dashboard measures crime volume rather than population-adjusted crime rates.

---

# 37. Why Crime Counts Are Not Population-Normalized

The dashboard currently compares absolute crime counts between LAPD areas.

This answers:

> Where were the most crimes recorded?

It does not answer:

> Which area has the highest crime rate per resident?

That second question would require population data for comparable geographic units.

Adding population data without carefully matching geographic boundaries could create misleading statistics, so it was intentionally left outside the current project scope.

---

# 38. AI Usage

An AI assistant was used throughout the assignment as an engineering support tool.

The complete AI interaction transcript is included under:

```text
ai_transcript/
```

AI was used during several stages of the project.

## Assignment interpretation

The assignment requirements were reviewed and translated into a concrete architecture including:

```text
API
Raw Layer
Transformation Layer
Quality Layer
Analytics Layer
Streamlit
Documentation
```

## API and product design

AI was used to discuss:

* API suitability
* dataset structure
* analytical opportunities
* business questions
* project scope

## ETL design

The AI assistant helped review decisions including:

* which columns to retain
* which columns to remove
* use of `crime_id`
* date transformations
* demographic transformations
* code-description validation
* creation of separate analytical outputs

## Challenging AI suggestions

The AI output was not accepted automatically.

For example, the initial suggestion was to save raw data as:

```text
.json.gz
```

This decision was questioned because the assignment prioritizes clarity and reviewer accessibility.

After reviewing the tradeoff, the raw storage format was changed to:

```text
.json
```

This allowed the source extract to remain directly inspectable.

This interaction demonstrates that AI suggestions were evaluated rather than copied blindly.

## Debugging

AI was also used while debugging Streamlit execution.

The application was initially executed using:

```bash
python app.py
```

which generated Streamlit runtime warnings.

The issue was identified and corrected to:

```bash
streamlit run app.py
```

Deprecated Streamlit parameters were also identified and replaced with current alternatives.

## Dashboard design

AI supported discussion around:

* dashboard filtering
* chart selection
* KPI design
* dynamic aggregation
* user-facing error handling
* data-quality presentation

## Documentation

AI was used to help structure the final project documentation while the implementation decisions and project scope were reviewed during development.

---

# 39. Git Usage

Git should be used throughout development rather than only at the end of the assignment.

Example logical commit progression:

```text
Initial project structure

Add API extraction

Add raw data persistence

Add schema validation

Add transformation layer

Add data quality checks

Add analytical outputs

Add Streamlit dashboard

Add interactive filters and KPIs

Improve dashboard error handling

Add README documentation

Add AI transcript
```

Using multiple meaningful commits demonstrates the evolution of the project and makes implementation decisions easier to review.

---

# 40. Reproducibility

The project is designed so a reviewer does not need to:

* manually download the dataset
* create a database
* install Docker
* configure cloud infrastructure
* obtain private credentials
* prepare local files manually

The complete execution sequence is:

```bash
git clone <repository-url>

cd <repository-folder>

python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

macOS/Linux:

```bash
source .venv/bin/activate
```

Then:

```bash
pip install -r requirements.txt

python etl.py

streamlit run app.py
```

---

# 41. What I Would Improve With More Time

Several improvements could be added while preserving the current architecture.

## Automated testing

Add unit tests for:

* schema validation
* age cleaning
* sex mappings
* military-time conversion
* duplicate handling
* coordinate validation
* analytical aggregations

## Logging

Move application logs into persistent local log files in addition to terminal output.

## More detailed quality severity levels

Quality checks could be classified as:

```text
INFO
WARNING
ERROR
```

instead of presenting only counts.

## Incremental extraction

If the source supported reliable incremental retrieval, the ETL could download only new or changed records instead of performing a full refresh.

## Geographic visualization

A map could be introduced using valid latitude and longitude fields.

This would require careful consideration of records with missing or obfuscated coordinates.

## External population data

Population information could allow analysis such as:

```text
crimes per 10,000 residents
```

rather than only absolute crime counts.

## Crime timing analysis

The existing `occurrence_hour` field could support analysis by:

```text
morning
afternoon
evening
night
```

## Additional demographic analysis

Victim descent could be analyzed, subject to careful interpretation and appropriate communication of missing or unknown values.

## More advanced anomaly detection

Monthly crime levels could be compared against rolling averages or historical ranges to identify unusual periods.

## Deployment

The Streamlit application could eventually be deployed to a hosted environment.

For this assignment, local execution was intentionally kept as the primary deployment method because the instructions prioritize reproducibility without external infrastructure.

---

# 42. Summary

This project demonstrates a complete local data-product workflow:

```text
Extract
   ↓
Persist Raw Data
   ↓
Validate
   ↓
Transform
   ↓
Check Data Quality
   ↓
Create Analytical Outputs
   ↓
Calculate Business Metrics
   ↓
Present With Streamlit
```

The implementation intentionally favors:

* clarity
* reproducibility
* maintainability
* explicit validation
* separation of responsibilities
* useful business analysis

over unnecessary infrastructure or architectural complexity.

The result is a self-contained data product that can be cloned, executed locally, reviewed, and extended without external databases, paid services, private credentials, or manual data preparation.
