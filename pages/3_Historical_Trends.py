import streamlit as st
import pandas as pd
import plotly.express as px

#ALL PAGES -----------------------------------------------------------------
from utils import load_data_and_update_sidebar

# Load data and update sidebar
load_data_and_update_sidebar()

# Access data
df_current = st.session_state.df_current
df_history = st.session_state.df_history

# Page title
st.title("Historical Trends")
st.markdown("Historical trends")

# ------------------------------------------------------------------------

# Timeline of studies
df_current['StartDate'] = pd.to_datetime(df_current['StartDate'], errors='coerce')
timeline_data = df_current.groupby(df_current['StartDate'].dt.year)['NCTId'].count().reset_index()
timeline_data.columns = ['Year', 'Studies']

fig_timeline = px.line(
    timeline_data,
    x="Year",
    y="Studies",
    markers=True,
    title="Number of Studies Initiated Over Time",
)
st.plotly_chart(fig_timeline, use_container_width=True)
