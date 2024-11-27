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
st.title("Competitive Analysis")
st.markdown("### Compare Sponsors and Focus Areas")

# ------------------------------------------------------------------------

# Sponsors' study count
top_sponsors = df_current['LeadSponsorName'].value_counts().nlargest(10)
fig_sponsors = px.bar(
    top_sponsors,
    orientation='h',
    title="Top 10 Sponsors by Study Count",
)
st.plotly_chart(fig_sponsors, use_container_width=True)
