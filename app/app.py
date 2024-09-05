from duckdb_connection import DuckDBConnection
import streamlit as st
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

#configs
st.set_page_config(
    page_title="NOAA Fishing Survey",
    page_icon="🎣",
    layout="wide",
    menu_items={
        'About': """This web app allows interactive exploratory data analysis 
                    for NOAA recreational saltwater fishing survey data
                    [https://github.com/lopezj1/noaa_eda] (https://github.com/lopezj1/noaa_eda)"""
    }
)

#constants
SCHEMA = 'analytics'
DUCKDB_PATH = str(Path().resolve() / "data/noaa_dw.duckdb")

@st.cache_resource
def get_duckdb_connection():
    return st.connection("duckdb", type=DuckDBConnection, database=DUCKDB_PATH)

conn = get_duckdb_connection()

@st.cache_data
def get_summary_data():
    return conn.query(f"""select 
        min(trip_year), 
        max(trip_year), 
        count(*),
        cast(sum(total_number_fish_caught) as int)
        from {SCHEMA}.trip_details
    """)

@st.cache_data
def get_run_chart_data():
    return conn.query(f"""
        select 
        trip_year, 
        count(*) as total_trips,
        cast(sum(total_number_fish_caught) as int) as total_fish
        from {SCHEMA}.trip_details
        group by trip_year
        order by trip_year asc
    """)

@st.cache_data
def get_tree_map_data():
    return conn.query(f"""
        select 
        us_region,    
        fishing_season, 
        fishing_method_uncollapsed,
        species_common_name,
        cast(total_number_fish_caught as int) as total_fish
        from {SCHEMA}.trip_details
    """)

@st.cache_data
def get_region_data():
    return conn.query(f"select * from {SCHEMA}.region_catches")

@st.cache_data
def get_season_data():
    return conn.query(f"select * from {SCHEMA}.season_catches")

@st.cache_data
def get_method_data():
    return conn.query(f"select * from {SCHEMA}.method_catches")

@st.cache_data
def get_top_species_trip_data():
    return conn.query(f"""
        select
        trip_date,
        trip_year,
        species_common_name,
        us_region,
        fishing_season,
        fishing_method_uncollapsed,
        total_length_fish_harvested_mm,
        total_weight_fish_harvested_kg, 
        trip_fishing_effort_hours,
        number_of_outings_in_last_2_months,
        number_of_outings_in_last_year,
        total_number_fish_caught
        from {SCHEMA}.trip_details 
        where species_common_name in (select species_common_name from {SCHEMA}.top_species)
    """)

#app
st.title(':blue[NOAA Recreational Saltwater Fishing Survey Data Analysis]')
st.divider()

df = get_summary_data()
start_year = df.iat[0,0]
end_year = df.iat[0,1]
total_trips = df.iat[0,2]
total_fish = df.iat[0,3]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Date Range", value=f"{start_year}-{end_year}")
with col2:
    st.metric(label="Total Number of Fishing Trips", value=f"{total_trips:,}")
with col3:
    st.metric(label="Total Number of Fish Caught", value=f"{total_fish:,}")

tab1, tab2, tab3 = st.tabs(["Overview", "Stats", "Trends"])
with tab1:
    # DRY up df transforms
    with st.expander("Run Chart of Trips & Catches", expanded=True):
        df = get_run_chart_data()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df['trip_year'], y=df['total_trips'],
                            mode='lines+markers',
                            name='# Trips'))
        fig.add_trace(go.Scatter(x=df['trip_year'], y=df['total_fish'],
                            mode='lines+markers',
                            name='# Fish Caught'))
        st.plotly_chart(fig, theme="streamlit", use_container_width=True)

    with st.expander("Treemap of Catches", expanded=True):
        df = get_tree_map_data()
        df = df.dropna() #drop non-leaves rows
        fig = px.treemap(df, path=[px.Constant("all"), 
                                   'us_region', 
                                   'fishing_season', 
                                   'fishing_method_uncollapsed',
                                   'species_common_name'],
                                #    color='us_region',
                                   values='total_fish')
        fig.data[0].textinfo = 'label+text+value'
        fig.update_traces(root_color="lightgrey")
        fig.update_layout(margin = dict(t=50, l=25, r=25, b=25))
        st.plotly_chart(fig, theme="streamlit", use_container_width=True)

with tab2:
    with st.expander("Catch Rate for Top 10 Targeted Species by US Region", expanded=True):
        df = get_region_data()
        df = df.rename(columns={"species_common_name": "Species Common Name"})
        df = df.set_index("Species Common Name")
        df = df.select_dtypes(exclude=['datetime', 'object']) * 100
        keys = df.columns.tolist()
        st.dataframe(df.style.format('{:.2f}%').highlight_max(axis=1))

    with st.expander("Catch Rate for Top 10 Targeted Species by Fishing Season", expanded=True):
        df = get_season_data()
        df = df.rename(columns={"species_common_name": "Species Common Name"})
        df = df.set_index("Species Common Name")
        df = df.select_dtypes(exclude=['datetime', 'object']) * 100
        st.dataframe(df.style.format('{:.2f}%').highlight_max(axis=1))

    with st.expander("Catch Rate for Top 10 Targeted Species by Fishing Method", expanded=True):
        df = get_method_data()
        df = df.rename(columns={"species_common_name": "Species Common Name"})
        df = df.set_index("Species Common Name")
        df = df.select_dtypes(exclude=['datetime', 'object']) * 100
        st.dataframe(df.style.format('{:.2f}%').highlight_max(axis=1))

with tab3:
    df = get_top_species_trip_data()
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
                                        index=0,
                                        options=['total_length_fish_harvested_mm',
                                                'total_weight_fish_harvested_kg', 
                                                'trip_fishing_effort_hours',
                                                'number_of_outings_in_last_2_months',
                                                'number_of_outings_in_last_year'
                                                ])
        with col2:
            y_axis_value = st.selectbox(label='Select Y Axis',
                                        index=1,
                                        options=['total_length_fish_harvested_mm',
                                                'total_weight_fish_harvested_kg', 
                                                'trip_fishing_effort_hours',
                                                'number_of_outings_in_last_2_months',
                                                'number_of_outings_in_last_year'
                                                ])
        with col3:
            size_value = st.selectbox(label='Select Size Variable',
                                        index=0,
                                        options=['total_number_fish_caught'])
            
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