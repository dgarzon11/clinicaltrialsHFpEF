import requests
import json
import streamlit as st

def download_studies(num_studies=10):
    base_url = "https://clinicaltrials.gov/api/v2/studies"
    params = {
        "format": "json",  # Specify the response format
        "pageSize": num_studies,  # Number of studies to retrieve
        "fields": "NCTId,BriefTitle,OverallStatus,StudyType,Phase,Condition"  # Fields to include
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        # Extract the relevant data
        studies = data.get('studies', [])

        # Check if studies are present
        if not studies:
            st.warning("No studies found. Please try again with a different number of studies.")
            return

        # Write the data to a JSON file
        file_name = "studies_sample.json"
        with open(file_name, mode="w") as f:
            json.dump(studies, f, indent=2)
        st.success(f"Successfully downloaded and saved {num_studies} studies to {file_name}.")

    except requests.RequestException as e:
        st.error(f"An error occurred: {e}")
        if e.response is not None:
            st.error(f"Response content: {e.response.text}")

if __name__ == "__main__":
    st.title("Clinical Trials Data Downloader")
    num_studies = st.number_input("Number of studies to download", min_value=1, max_value=100, value=10)
    if st.button("Download Studies"):
        download_studies(num_studies)
