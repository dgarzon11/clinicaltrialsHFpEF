import streamlit as st
import pandas as pd
import plotly.express as px

# Load datasets
df_current = pd.read_csv("data/dmd_current.csv")

st.title("Historical Trends")
st.markdown("### Analyze Clinical Trial Trends Over Time")

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
