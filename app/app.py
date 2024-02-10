from duckdb_connection import DuckDBConnection
import streamlit as st
from pathlib import Path
import pandas as pd

#configs
st.set_page_config(
    page_title="NOAA Fishing Survey",
    page_icon="🎣",
    layout="centered",
    menu_items={
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

#constants
SCHEMA = 'analytics'
DUCKDB_PATH = str(Path(__file__).resolve().parent.parent / "duckdb" / "noaa_dw.duckdb")

#connections
conn = st.connection("duckdb", type=DuckDBConnection, database=DUCKDB_PATH)

#app
st.title(':blue[NOAA Recreational Fishing Survey]')
st.divider()

df = conn.query(f"select min(trip_year), max(trip_year), count(*) from {SCHEMA}.stg_noaa__trips")
start_year = df.iat[0,0]
end_year = df.iat[0,1]
total_trips = df.iat[0,2]

col1, col2 = st.columns(2)
with col1:
    st.metric(label="Date Range", value=f"{start_year}-{end_year}")
with col2:
    st.metric(label="Total Fishing Trips", value=f"{total_trips}") #format with commas

tab1, tab2 = st.tabs(["Summary Statistics", "Historical Trends"])
with tab1:
    #format all values as percentage with 2 decimal places
    with st.expander("Catch Rate for Top 10 Targeted Species by US Region"):
        df = conn.query(f"select * from {SCHEMA}.region_catches")
        df = df.set_index("species_common_name")
        st.dataframe(df.style.highlight_max(axis=0))

    with st.expander("Catch Rate for Top 10 Targeted Species by Fishing Season"):
        df = conn.query(f"select * from {SCHEMA}.season_catches")
        df = df.set_index("species_common_name")
        st.dataframe(df.style.highlight_max(axis=0))

    with st.expander("Catch Rate for Top 10 Targeted Species by Fishing Method", expanded=True):
        df = conn.query(f"select * from {SCHEMA}.method_catches")
        df = df.set_index("species_common_name")
        st.dataframe(df.style.highlight_max(axis=0))