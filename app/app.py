import pandas as pd
import polars as pl
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

#---------------Polars Approach-------------------
# tweak the streaming engine chunk_size
# pl.Config.set_streaming_chunk_size(5000*13)

# df_obt = pl.scan_parquet(f'{DATA_PATH}/analytics.trip_details.parquet'
#                             ,low_memory=True
#                             # ,use_pyarrow=True
#                             # ,memory_map=True
#                             )
# print(df_obt.explain())
# df_obt = df_obt.collect(streaming=True).to_pandas()

#--------------Parquet Method 1--------------------------
# parquet_file = pq.ParquetFile(f'{DATA_PATH}/analytics.trip_details.parquet')
# for i in parquet_file.iter_batches(batch_size=10):
#     print("RecordBatch")
#     print(i.to_pandas())

#--------------Parquet Method 2--------------------------
def read_parquet_batches(file_path, batch_size=1000):
    file = pq.ParquetFile(file_path)
    num_row_groups = file.num_row_groups
    print(f'Total Row Groups: {num_row_groups}')
    
    current_batch = []
    total_rows = 0

    for row_group_idx in range(num_row_groups):
        df = file.read_row_group(row_group_idx).to_pandas()
        for _, row in df.iterrows():
            current_batch.append(row)
            total_rows += 1
            if total_rows % batch_size == 0:
                yield pd.DataFrame(current_batch)
                current_batch = []
    
    # Yield the remaining rows if the total number of rows is not divisible by batch_size
    if current_batch:
        yield pd.DataFrame(current_batch)

file_path = f'{DATA_PATH}/analytics.trip_details.parquet'
batch_size = 1000

generator = read_parquet_batches(file_path, batch_size=batch_size)
for i, batch_data in enumerate(generator):
    # Process batch_data
    print(f"Batch {i+1}:")
    print(batch_data.shape[0])
    # Your processing logic here

#--------------Dataframes used for Analysis
# df_obt = pd.concat(dfs, ignore_index=True) #cannot concatenate all at once
df_region = pd.read_parquet(f'{DATA_PATH}/analytics.region_catches.parquet')
df_season = pd.read_parquet(f'{DATA_PATH}/analytics.season_catches.parquet')
df_method = pd.read_parquet(f'{DATA_PATH}/analytics.method_catches.parquet')
df_species = pd.read_parquet(f'{DATA_PATH}/analytics.top_species.parquet')

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
