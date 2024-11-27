import streamlit as st

#ALL PAGES -----------------------------------------------------------------
from utils import load_data_and_update_sidebar

# Load data and update sidebar
load_data_and_update_sidebar()

# Access data
df_current = st.session_state.df_current
df_history = st.session_state.df_history

# Page title
st.title("Insights & Recommendations")
st.markdown("Monitor and analyze clinical trials data in real-time.")

# ------------------------------------------------------------------------

# Example insights
st.markdown("- **Insight 1**: Sponsor A is leading in ongoing studies.")
st.markdown("- **Insight 2**: Region X shows a lack of active studies, indicating potential opportunities.")
