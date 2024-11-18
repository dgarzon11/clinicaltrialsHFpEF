import json
import csv
import datetime
import os
import requests

def download_studies(page_size):
    base_url = "https://clinicaltrials.gov/api/v2/studies?query.cond=duchenne"
    params = {
        "format": "json",
        "pageSize": page_size,
        "fields": (
            "NCTId,"
            "BriefTitle,"
            "OverallStatus,"
            "StudyType,"
            "Phase,"
            "Condition,"
            "LeadSponsorName,"
            "StartDate,"
            "StudyFirstPostDate,"
            "LastUpdatePostDate"
        )
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        studies = data.get('studies', [])

        if not studies:
            print("No studies found. Please try again with a different number of studies.")
            return

        os.makedirs('data', exist_ok=True)
        file_name = os.path.join('data', "duchenne_studies_current.json")
        with open(file_name, mode="w") as f:
            json.dump(studies, f, indent=2)
        print(f"Successfully downloaded and saved {len(studies)} studies to {file_name}.")

    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")

def json_to_csv(json_file, csv_file):
    with open(json_file, 'r') as f:
        data = json.load(f)

    csv_columns = [
        'NCTId',
        'BriefTitle',
        'OverallStatus',
        'StudyType',
        'Phases',
        'Conditions',
        'LeadSponsorName',
        'StartDate',
        'StudyFirstPostDate',
        'LastUpdatePostDate',
        'Timestamp'
    ]

    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()

        timestamp = datetime.datetime.now().isoformat()

        for study in data:
            protocol = study.get('protocolSection', {})
            identification = protocol.get('identificationModule', {})
            status = protocol.get('statusModule', {})
            sponsor = protocol.get('sponsorCollaboratorsModule', {})
            conditions_module = protocol.get('conditionsModule', {})
            design = protocol.get('designModule', {})

            nct_id = identification.get('nctId', '')
            brief_title = identification.get('briefTitle', '')
            overall_status = status.get('overallStatus', '')
            study_type = design.get('studyType', '')
            phases = ', '.join(design.get('phases', []))
            conditions = ', '.join(conditions_module.get('conditions', []))
            lead_sponsor = sponsor.get('leadSponsor', {}).get('name', '')
            start_date = status.get('startDateStruct', {}).get('date', '')
            study_first_post_date = status.get('studyFirstPostDateStruct', {}).get('date', '')
            last_update_post_date = status.get('lastUpdatePostDateStruct', {}).get('date', '')

            writer.writerow({
                'NCTId': nct_id,
                'BriefTitle': brief_title,
                'OverallStatus': overall_status,
                'StudyType': study_type,
                'Phases': phases,
                'Conditions': conditions,
                'LeadSponsorName': lead_sponsor,
                'StartDate': start_date,
                'StudyFirstPostDate': study_first_post_date,
                'LastUpdatePostDate': last_update_post_date,
                'Timestamp': timestamp
            })

    print(f"Data has been successfully written to {csv_file}.")

def append_to_history(current_csv, history_csv):
    file_exists = os.path.isfile(history_csv)

    with open(history_csv, 'a', newline='', encoding='utf-8') as history_file:
        with open(current_csv, 'r', newline='', encoding='utf-8') as current_file:
            reader = csv.reader(current_file)
            writer = csv.writer(history_file)

            header = next(reader)

            if not file_exists:
                writer.writerow(header)

            for row in reader:
                writer.writerow(row)

    print(f"Data from {current_csv} has been appended to {history_csv}.")

if __name__ == "__main__":
    download_studies(3)
    json_file = os.path.join('data', 'duchenne_studies_current.json')
    csv_file = os.path.join('data', 'duchenne_studies_current.csv')
    history_csv = os.path.join('data', 'duchenne_studies_history.csv')

    json_to_csv(json_file, csv_file)
    append_to_history(csv_file, history_csv)

