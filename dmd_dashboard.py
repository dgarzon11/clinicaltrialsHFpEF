import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Load datasets
df_current = pd.read_csv("data/dmd_current.csv")
df_history = pd.read_csv("data/dmd_history.csv")

# Page title
st.title("DMD Clinical Trials Dashboard")

# Key metrics
st.subheader("Key Metrics")
total_studies = df_current['NCTId'].nunique()
completed_studies = df_current[df_current['OverallStatus'] == "COMPLETED"]['NCTId'].nunique()
ongoing_studies = df_current[df_current['OverallStatus'].str.contains("RECRUITING", na=False)]['NCTId'].nunique()

st.metric("Total Studies", total_studies)
st.metric("Completed Studies", completed_studies)
st.metric("Ongoing Studies", ongoing_studies)

# Latest study timestamp
st.markdown("### Latest Study Update")
latest_timestamp = pd.to_datetime(df_current['Timestamp']).max().strftime('%d %B %Y %H:%M')
st.info(f"Data last updated on: {latest_timestamp}")

# Studies by Lead Sponsor
st.markdown("### Studies by Lead Sponsor")
top_sponsors = df_current['LeadSponsorName'].value_counts().nlargest(5)
fig_sponsors = px.bar(
    top_sponsors.sort_values(),
    orientation='h',
    labels={"index": "Lead Sponsor", "value": "Number of Studies"},
    title="Top 5 Lead Sponsors by Study Count",
)
st.plotly_chart(fig_sponsors, use_container_width=True)

# Distribution of OverallStatus
st.markdown("### Study Status Distribution")
status_distribution = df_current['OverallStatus'].value_counts()
fig_status = px.pie(
    values=status_distribution.values,
    names=status_distribution.index,
    title="Distribution of Study Status",
)
st.plotly_chart(fig_status, use_container_width=True)

# Timeline of studies
st.markdown("### Studies Timeline")
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

# Study details
st.markdown("### Study Details")
status_filter = st.selectbox("Filter by Status", options=df_current['OverallStatus'].unique(), index=0)
filtered_data = df_current[df_current['OverallStatus'] == status_filter]
st.dataframe(filtered_data[['NCTId', 'BriefTitle', 'LeadSponsorName', 'OverallStatus', 'StartDate']])
