import requests
import json

def download_studies():
    base_url = "https://clinicaltrials.gov/api/v2/studies?query.cond=duchenne"
    params = {
        "format": "json",  # Specify the response format
        "pageSize": 5,  # Number of studies to retrieve
        "fields": "NCTId,BriefTitle,OverallStatus,StudyType,Phase,Condition,LeadSponsorName,StartDate,StudyFirstPostDate,LastUpdatePostDate"  # Fields to include
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        # Extract the relevant data
        studies = data.get('studies', [])

        # Check if studies are present
        if not studies:
            print("No studies found. Please try again with a different number of studies.")
            return

        # Write the data to a JSON file
        file_name = "duchenne_studies_sample.json"
        with open(file_name, mode="w") as f:
            json.dump(studies, f, indent=2)
        print(f"Successfully downloaded and saved {len(studies)} studies to {file_name}.")

    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")

if __name__ == "__main__":
    download_studies()

