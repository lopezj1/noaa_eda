import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path
import pyarrow.parquet as pq

#configs
st.set_page_config(
    page_title="NOAA Fishing Survey",
    page_icon="🎣",
    layout="centered",
    menu_items={
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

#constants/home/jlopez/de_projects/noaa_eda/app/data
DATA_PATH = Path(__file__).resolve().parent / "data"
print(DATA_PATH)

#read parquet files into pandas dataframe
# df_obt = pd.read_parquet('/home/jlopez/de_projects/noaa_eda/app/data/noaa_obt.parquet', "chunks_[1-2]*", engine="fastparquet")
parquet_file = pq.ParquetFile(f'{DATA_PATH}/noaa_obt.parquet')
for i in parquet_file.iter_batches(batch_size=10):
    print("RecordBatch")
    print(i.to_pandas())

# df_region = pd.read_parquet(f'{DATA_PATH}/noaa_obt.parquet')
df_region = pd.read_parquet(f'{DATA_PATH}/noaa_region.parquet')
df_season = pd.read_parquet(f'{DATA_PATH}/noaa_season.parquet')
df_method = pd.read_parquet(f'{DATA_PATH}/noaa_method.parquet')
df_species = pd.read_parquet(f'{DATA_PATH}/noaa_species.parquet')

def format_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"species_common_name": "Species Common Name"})
    df = df.set_index("Species Common Name")
    df = df.select_dtypes(exclude=['datetime', 'object']) * 100

#app
st.title(':blue[NOAA Recreational Fishing Survey]')
st.divider()

start_year = df_obt.trip_year.min()
end_year = df_obt.trip_year.max()
total_trips = df_obt.size

col1, col2 = st.columns(2)
with col1:
    st.metric(label="Date Range", value=f"{start_year}-{end_year}")
with col2:
    st.metric(label="Total Fishing Trips", value=f"{total_trips:,}")

tab1, tab2 = st.tabs(["Summary Statistics", "Historical Trends"])
with tab1:
    with st.expander("Catch Rate for Top 10 Targeted Species by US Region"):
        df = format_summary_df(df_region)
        st.dataframe(df.style.format('{:.2f}%').highlight_max(axis=1))

    with st.expander("Catch Rate for Top 10 Targeted Species by Fishing Season"):
        df = format_summary_df(df_season)
        st.dataframe(df.style.format('{:.2f}%').highlight_max(axis=1))

    with st.expander("Catch Rate for Top 10 Targeted Species by Fishing Method", expanded=True):
        df = format_summary_df(df_method)
        st.dataframe(df.style.format('{:.2f}%').highlight_max(axis=1))

with tab2:
    top_species = df_species.species_common_name.tolist()
    df = df_obt.query('species_common_name in @top_species')
    df = df.sort_values('trip_date', ascending=True)

    with st.expander("Select Filters for Charting"):
        #need a callback with on_change property to update the color option on the chart to update
        col1, col2, col3 = st.columns(3)
        with col1:
            regions = df['us_region'].unique().tolist()
            region_filter = options = st.multiselect(label='Select Region(s)',
                                                    options=regions,
                                                    default=regions
                                                    )
        with col2:
            seasons = df['fishing_season'].unique().tolist()
            season_filter = options = st.multiselect(label='Select Season(s)',
                                                    options=seasons,
                                                    default=seasons
                                                    )
        with col3:
            methods = df['fishing_method_uncollapsed'].unique().tolist()
            method_filter = options = st.multiselect(label='Select Fishing Method(s)',
                                                    options=methods,
                                                    default=methods
                                                    )

    with st.expander("Select Data for Charting"):
        col1, col2, col3 = st.columns(3)
        with col1:
            x_axis_value = st.selectbox(label='Select X Axis',
                                        index=2,
                                        options=['fish_weight_lbs',
                                                'fish_weight_kg', 
                                                'fish_length_in',
                                                'fish_length_cm',
                                                'trip_fishing_effort_hours',
                                                'number_of_outings_in_last_2_months',
                                                'number_of_outings_in_last_year',
                                                'total_number_fish_caught'])
        with col2:
            y_axis_value = st.selectbox(label='Select Y Axis',
                                        index=0,
                                        options=['fish_weight_lbs',
                                                'fish_weight_kg', 
                                                'fish_length_in',
                                                'fish_length_cm',
                                                'trip_fishing_effort_hours',
                                                'number_of_outings_in_last_2_months',
                                                'number_of_outings_in_last_year',
                                                'total_number_fish_caught'])
        with col3:
            size_value = st.selectbox(label='Select Size Variable',
                                        index=0,
                                        options=[])
            
    fig = px.scatter(
        df.query('us_region in @region_filter & fishing_season in @season_filter & fishing_method_uncollapsed in @method_filter'),
        x=x_axis_value,
        y=y_axis_value,
        color="species_common_name",
        size=size_value,
        animation_frame="trip_year",
        # animation_group="species_common_name"
    )
    # fig.update_layout(transition = {'duration': 10000})
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
