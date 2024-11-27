import streamlit as st
import pandas as pd

#ALL PAGES -----------------------------------------------------------------
from utils import load_data_and_update_sidebar

# Load data and update sidebar
load_data_and_update_sidebar()

# Access data
df_current = st.session_state.df_current
df_history = st.session_state.df_history

# Page title
st.title("Welcome to the DMD Clinical Trials Dashboard")
st.markdown("Monitor and analyze clinical trials data in real-time.")

# ------------------------------------------------------------------------

st.markdown(
    """
    Use the menu on the left to navigate through the different sections of the dashboard:
    - **Overview**: Key metrics and general insights.
    - **Study Details**: Explore detailed data for current and historical studies.
    - **Historical Trends**: Analyze how clinical trials have evolved over time.
    - **Geographic Distribution**: Discover the geographic spread of clinical trials.
    - **Competitive Analysis**: Compare sponsors and their areas of focus.
    - **Download Reports**: Export datasets and visualizations.
    - **Insights & Recommendations**: Key findings and actionable strategies.
    """
)


