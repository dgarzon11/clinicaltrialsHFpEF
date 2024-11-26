import streamlit as st
import pandas as pd
import plotly.express as px

# Load datasets
df_current = pd.read_csv("data/dmd_current.csv")

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
