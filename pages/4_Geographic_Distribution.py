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
st.title("Geographic Distribution")
st.markdown("Monitor and analyze clinical trials data in real-time.")

# ------------------------------------------------------------------------

st.title("Geographic Distribution")
st.markdown("### Visualize the Locations of Clinical Trials")

# Map visualization
if "Location" in df_current.columns:
    fig_map = px.scatter_geo(
        df_current,
        locations="Location",
        title="Geographic Distribution of Studies",
    )
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.warning("Location data is not available in the dataset.")
