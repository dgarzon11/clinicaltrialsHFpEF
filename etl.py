import json
import csv
import datetime
import os
import requests
import pandas as pd

def download_studies(page_size):
    base_url = "https://clinicaltrials.gov/api/v2/studies?query.cond=duchenne"
    params = {
        "format": "json",
        "pageSize": page_size,
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
        file_name = os.path.join('data', "dmd_current.json")
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
        'NCTId', 'BriefTitle', 'Acronym', 'OverallStatus', 'BriefSummary',
        'HasResults', 'Condition', 'InterventionType', 'InterventionName',
        'PrimaryOutcomeMeasure', 'SecondaryOutcomeMeasure', 'LeadSponsorName',
        'CollaboratorName', 'Sex', 'MinimumAge', 'MaximumAge', 'StdAge', 'Phase',
        'EnrollmentCount', 'LeadSponsorClass', 'StudyType', 'DesignPrimaryPurpose',
        'OrgStudyId', 'SecondaryId', 'StartDate', 'PrimaryCompletionDate',
        'CompletionDate', 'StudyFirstPostDate', 'ResultsFirstPostDate',
        'LastUpdatePostDate', 'Timestamp'
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
            outcomes = protocol.get('outcomesModule', {})
            interventions = protocol.get('armsInterventionsModule', {}).get('interventions', [])
            eligibility = protocol.get('eligibilityModule', {})
            conditions = protocol.get('conditionsModule', {})
            design = protocol.get('designModule', {})
            description = protocol.get('descriptionModule', {})

            row = {
                'NCTId': identification.get('nctId', ''),
                'BriefTitle': identification.get('briefTitle', ''),
                'Acronym': identification.get('acronym', ''),
                'OverallStatus': status.get('overallStatus', ''),
                'BriefSummary': description.get('briefSummary', ''),
                'HasResults': study.get('hasResults', ''),
                'Condition': ', '.join(conditions.get('conditions', [])),
                'InterventionType': ', '.join(i.get('type', '') for i in interventions),
                'InterventionName': ', '.join(i.get('name', '') for i in interventions),
                'PrimaryOutcomeMeasure': ', '.join(o.get('measure', '') for o in outcomes.get('primaryOutcomes', [])),
                'SecondaryOutcomeMeasure': ', '.join(o.get('measure', '') for o in outcomes.get('secondaryOutcomes', [])),
                'LeadSponsorName': sponsor.get('leadSponsor', {}).get('name', ''),
                'CollaboratorName': ', '.join(c.get('name', '') for c in sponsor.get('collaborators', [])),
                'Sex': eligibility.get('sex', ''),
                'MinimumAge': eligibility.get('minimumAge', ''),
                'MaximumAge': eligibility.get('maximumAge', ''),
                'StdAge': ', '.join(eligibility.get('stdAges', [])),
                'Phase': ', '.join(design.get('phases', [])),
                'EnrollmentCount': design.get('enrollmentInfo', {}).get('count', ''),
                'LeadSponsorClass': sponsor.get('leadSponsor', {}).get('class', ''),
                'StudyType': design.get('studyType', ''),
                'DesignPrimaryPurpose': design.get('designInfo', {}).get('primaryPurpose', ''),
                'OrgStudyId': identification.get('orgStudyIdInfo', {}).get('id', ''),
                'SecondaryId': ', '.join(sid.get('id', '') for sid in identification.get('secondaryIdInfos', [])),
                'StartDate': status.get('startDateStruct', {}).get('date', ''),
                'PrimaryCompletionDate': status.get('primaryCompletionDateStruct', {}).get('date', ''),
                'CompletionDate': status.get('completionDateStruct', {}).get('date', ''),
                'StudyFirstPostDate': status.get('studyFirstPostDateStruct', {}).get('date', ''),
                'ResultsFirstPostDate': status.get('resultsFirstPostDateStruct', {}).get('date', ''),
                'LastUpdatePostDate': status.get('lastUpdatePostDateStruct', {}).get('date', ''),
                'Timestamp': timestamp
            }

            writer.writerow(row)

    print(f"Data has been successfully written to {csv_file}.")

def append_to_history(current_csv, history_csv):
    """
    Append rows from the current CSV to the history CSV, ensuring schema consistency.
    """
    expected_columns = 31

    # Load the current CSV and validate its structure
    current_data = pd.read_csv(current_csv)
    if current_data.shape[1] != expected_columns:
        print(f"Warning: The current file {current_csv} has an unexpected number of columns ({current_data.shape[1]}).")
        current_data = current_data.iloc[:, :expected_columns]  # Trim to expected columns

    # Validate history file structure
    file_exists = os.path.isfile(history_csv)
    if file_exists:
        history_data = pd.read_csv(history_csv)
        if history_data.shape[1] != expected_columns:
            print(f"Warning: The history file {history_csv} has an unexpected number of columns ({history_data.shape[1]}).")
            history_data = history_data.iloc[:, :expected_columns]  # Trim to expected columns
    else:
        history_data = pd.DataFrame(columns=current_data.columns)  # Create an empty DataFrame

    # Append current data to history
    combined_data = pd.concat([history_data, current_data], ignore_index=True)

    # Drop duplicate rows (optional)
    combined_data.drop_duplicates(inplace=True)

    # Save the updated history file
    combined_data.to_csv(history_csv, index=False)
    print(f"Data from {current_csv} has been appended to {history_csv} with schema validation.")

    
def generate_changes_last_n(history_csv, changes_csv, n):
    """
    Identify changes in the last n Timestamps for each NCTId and generate a CSV of changes.
    """
    expected_columns = 31

    # Load the history CSV and validate its structure
    df = pd.read_csv(history_csv)
    if df.shape[1] != expected_columns:
        print(f"Warning: The history file {history_csv} has an unexpected number of columns ({df.shape[1]}).")
        df = df.iloc[:, :expected_columns]  # Trim to expected columns

    # Ensure Timestamp is in datetime format
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='ISO8601', errors='coerce')
    df.dropna(subset=['Timestamp'], inplace=True)

    # Sort by NCTId and Timestamp
    df.sort_values(by=['NCTId', 'Timestamp'], inplace=True)

    # Detect changes
    changes = []
    for nct_id, group in df.groupby('NCTId'):
        group = group.tail(n).reset_index(drop=True)
        for i in range(1, len(group)):
            current_row = group.iloc[i]
            previous_row = group.iloc[i - 1]
            for column in group.columns:
                if column not in ['NCTId', 'Timestamp']:
                    current_value = current_row[column]
                    previous_value = previous_row[column]
                    if pd.notnull(current_value) and pd.notnull(previous_value) and current_value != previous_value:
                        changes.append({
                            'NCTId': nct_id,
                            'final_date': current_row['Timestamp'],
                            'start_date': previous_row['Timestamp'],
                            'change': column,
                            'final_value': current_value,
                            'start_value': previous_value
                        })

    # Create a DataFrame with changes
    changes_df = pd.DataFrame(changes)

    # Save the changes CSV
    changes_df.to_csv(changes_csv, index=False)
    print(f"Changes CSV generated: {changes_csv}")

if __name__ == "__main__":
    download_studies(100000)
    json_file = os.path.join('data', 'dmd_current.json')
    csv_file = os.path.join('data', 'dmd_current.csv')
    history_csv = os.path.join('data', 'dmd_history.csv')

    json_to_csv(json_file, csv_file)
    append_to_history(csv_file, history_csv)
    generate_changes_last_n(history_csv, os.path.join('data', 'dmd_changes.csv'),10)
