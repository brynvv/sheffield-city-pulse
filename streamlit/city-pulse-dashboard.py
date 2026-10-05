import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path


# ============================================================
# Page configuration
# ============================================================

st.set_page_config(
    page_title="Air Quality Dashboard",
    page_icon="🌍",
    layout="wide",
)


# ============================================================
# Data file
# ============================================================

DATA_FILE = Path("data/processed/city_metrics.parquet")


# ============================================================
# Load data
# ============================================================

@st.cache_data
def load_data():

    query = f"""
        SELECT
            hour,
            avg_pm25,
            avg_temp,
            avg_wind
        FROM read_parquet('{DATA_FILE}')
        ORDER BY hour
    """

    return duckdb.sql(query).df()


# ============================================================
# Load
# ============================================================

st.title("🌍 Air Quality Dashboard")

try:

    df = load_data()

except Exception as e:

    st.error("There was a problem loading the processed data.")
    st.exception(e)
    st.stop()


# ============================================================
# Prepare data
# ============================================================

df["hour"] = pd.to_datetime(df["hour"])

st.write(
    f"Loaded {len(df):,} records."
)


# ============================================================
# Sidebar
# ============================================================

st.sidebar.header("Filters")

min_date = df["hour"].min().date()
max_date = df["hour"].max().date()

date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)


# ============================================================
# Filter data
# ============================================================

df_filtered = df.copy()

if len(date_range) == 2:

    start_date, end_date = date_range

    df_filtered = df[
        (df["hour"].dt.date >= start_date)
        & (df["hour"].dt.date <= end_date)
    ].copy()


# ============================================================
# Summary metrics
# ============================================================

st.subheader("Summary")

col1, col2, col3, col4 = st.columns(4)

with col1:

    st.metric(
        "Average PM2.5",
        f"{df_filtered['avg_pm25'].mean():.1f}",
    )

with col2:

    st.metric(
        "Maximum PM2.5",
        f"{df_filtered['avg_pm25'].max():.1f}",
    )

with col3:

    st.metric(
        "Average temperature",
        f"{df_filtered['avg_temp'].mean():.1f}",
    )

with col4:

    st.metric(
        "Average wind",
        f"{df_filtered['avg_wind'].mean():.1f}",
    )


# ============================================================
# PM2.5
# ============================================================

st.subheader("PM2.5 over time")

pm25_data = (
    df_filtered
    .set_index("hour")[["avg_pm25"]]
)

st.line_chart(pm25_data)


# ============================================================
# Temperature
# ============================================================

st.subheader("Temperature over time")

temperature_data = (
    df_filtered
    .set_index("hour")[["avg_temp"]]
)

st.line_chart(temperature_data)


# ============================================================
# Wind
# ============================================================

st.subheader("Wind over time")

wind_data = (
    df_filtered
    .set_index("hour")[["avg_wind"]]
)

st.line_chart(wind_data)


# ============================================================
# PM2.5 vs wind
# ============================================================

st.subheader("PM2.5 vs wind")

st.scatter_chart(
    df_filtered,
    x="avg_wind",
    y="avg_pm25",
)


# ============================================================
# Raw data
# ============================================================

with st.expander("View processed data"):

    st.dataframe(
        df_filtered,
        use_container_width=True,
        hide_index=True,
    )