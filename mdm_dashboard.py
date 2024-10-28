#Streamlit app dashboard for MDM that visualizws data from duchenne_studies_sample.csv

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio  # Import the plotly.io module
from plotly.subplots import make_subplots        # Import the make_subplots function
import plotly.graph_objects as go                            # Import the plotly.graph_objects module       
pio.renderers.default = "browser"  # Set the default renderer to "browser"                                                                     

df = pd.read_csv("duchenne_studies_sample.csv")

st.title("DMD Clinical Trials Dashboard")

st.markdown("## Latest Study Timestamp")
max_timestamp = pd.to_datetime(df['Timestamp'].max()).strftime('%d %B %Y')
st.info(f"Last update: {max_timestamp}")

st.write(df)

st.markdown("## Filter by Phase")
phase_filter = st.selectbox('Select phase(s):', df['Phases'].unique())
filtered_df = df[df['Phases'].str.contains(phase_filter)]




