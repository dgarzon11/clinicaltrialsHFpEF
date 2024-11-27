import streamlit as st
import pandas as pd
import plotly.express as px

# Load datasets
# Fallback mechanism to ensure data is available
if "df_current" not in st.session_state:
    st.session_state.df_current = pd.read_csv("data/dmd_current.csv")
if "df_history" not in st.session_state:
    st.session_state.df_history = pd.read_csv("data/dmd_history.csv")

# Access data
df_current = st.session_state.df_current
df_history = st.session_state.df_history

df_current['StartDate'] = pd.to_datetime(df_current['StartDate'], errors='coerce')

# Page Title
st.title("Clinical Trials Dashboard")
st.markdown("Monitor and analyze clinical trials data in real-time.")

# Key Metrics
col1, col2, col3, col4 = st.columns(4)

total_trials = df_current['NCTId'].nunique()
recruiting_trials = df_current[df_current['OverallStatus'] == "RECRUITING"]['NCTId'].nunique()
completed_trials = df_current[df_current['OverallStatus'] == "COMPLETED"]['NCTId'].nunique()
success_rate = round((completed_trials / total_trials) * 100, 2) if total_trials > 0 else 0

col1.metric("Active Trials", total_trials, "+12%")
col2.metric("Recruiting", recruiting_trials, "+5%")
col3.metric("Completed", completed_trials, "+3%")
col4.metric("Success Rate", f"{success_rate}%", "-2%")

# Clinical Trials Trend
st.markdown("### Clinical Trials Trend")
df_timeline = df_current.groupby(df_current['StartDate'].dt.month)['NCTId'].count().reset_index()
df_timeline.columns = ['Month', 'Trials']

fig = px.line(
    df_timeline,
    x='Month',
    y='Trials',
    markers=True,
    title="Clinical Trials Trend",
    labels={"Month": "Month", "Trials": "Number of Trials"},
)
fig.update_traces(line=dict(color="blue", width=2), marker=dict(size=8))
fig.update_layout(title_x=0.5)

st.plotly_chart(fig, use_container_width=True)

# Recent Trials Section
st.markdown("### Recent Trials")
recent_trials = df_current.sort_values(by='StartDate', ascending=False).head(3)

for _, trial in recent_trials.iterrows():
    st.write(
        f"**{trial['BriefTitle']}**  \n"
        f"**NCT ID**: {trial['NCTId']}  \n"
        f"**Phase**: {trial.get('Phase', 'N/A')}  \n"
        f"**Status**: {trial['OverallStatus']}"
    )
    if trial['OverallStatus'] == "RECRUITING":
        st.success("Recruiting")
    elif trial['OverallStatus'] == "COMPLETED":
        st.info("Completed")
    else:
        st.warning(trial['OverallStatus'])
    st.divider()

# Add "View All" Button (Optional for Navigation)
if st.button("View All Trials"):
    st.write("Navigate to the Study Details page.")
