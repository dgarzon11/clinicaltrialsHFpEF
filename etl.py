"""
ETL Pipeline for Clinical Trials Data Processing
---------------------------------------------

This script implements an ETL (Extract, Transform, Load) pipeline for processing clinical trials data
specifically related to Duchenne Muscular Dystrophy (DMD) from clinicaltrials.gov.

The pipeline consists of four main components:
1. Data Extraction: Downloads clinical trials data from clinicaltrials.gov API
2. Data Transformation: Converts JSON data to a structured CSV format
3. Historical Data Management: Maintains a history of all data updates
4. Change Detection: Analyzes and records changes between data updates

Key Features:
- Fetches clinical trial data using the clinicaltrials.gov API v2
- Processes and standardizes various data fields including dates and measures
- Maintains a historical record of all data changes
- Generates detailed change logs for tracking updates
- Handles multiple data types and formats (JSON, CSV)

Dependencies:
- pandas: Data manipulation and analysis
- requests: HTTP requests to the API
- json: JSON data processing
- csv: CSV file operations
- datetime: Date and time operations
"""
import json
import csv
import datetime
import os
import requests
import pandas as pd
import re

def download_studies(page_size):
    """
    Downloads clinical trials data from clinicaltrials.gov API.
    Searches for studies related to both 'duchenne' and 'DMD' conditions.
    
    Args:
        page_size (int): Number of studies to retrieve
    
    Returns:
        None. Saves downloaded data to a JSON file in the data directory.
    """
    # Using OR operator to search for either duchenne or DMD in conditions
    base_url = "https://clinicaltrials.gov/api/v2/studies?query.cond=duchenne%20OR%20dmd"
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
        file_name = os.path.join('data', "studies.json")
        with open(file_name, mode="w") as f:
            json.dump(studies, f, indent=2)
        print(f"Successfully downloaded and saved {len(studies)} studies to {file_name}.")

    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")

def data_preparation(json_file, csv_file):
    """
    Transforms JSON clinical trials data into a structured CSV format.
    
    Args:
        json_file (str): Path to the source JSON file
        csv_file (str): Path where the CSV file will be saved
    
    Returns:
        None. Writes processed data to specified CSV file.
    """
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

            # Standardize dates and numeric values
            def standardize_date(date_str):
                if isinstance(date_str, str):
                    try:
                        if len(date_str.split('-')) == 2:  # Year-Month format
                            date_str += '-01'
                        return pd.to_datetime(date_str, errors='coerce').strftime('%Y-%m-%d')
                    except Exception:
                        return ''
                return ''
            row = {
                'NCTId': identification.get('nctId', ''),
                'BriefTitle': identification.get('briefTitle', ''),
                'Acronym': identification.get('acronym', ''),
                'OverallStatus': clean_status(status.get('overallStatus', '')),
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
                'StartDate': standardize_date(status.get('startDateStruct', {}).get('date', '')),
                'PrimaryCompletionDate': standardize_date(status.get('primaryCompletionDateStruct', {}).get('date', '')),
                'CompletionDate': standardize_date(status.get('completionDateStruct', {}).get('date', '')),
                'StudyFirstPostDate': standardize_date(status.get('studyFirstPostDateStruct', {}).get('date', '')),
                'ResultsFirstPostDate': standardize_date(status.get('resultsFirstPostDateStruct', {}).get('date', '')),
                'LastUpdatePostDate': standardize_date(status.get('lastUpdatePostDateStruct', {}).get('date', '')),
                'Timestamp': timestamp
            }
            writer.writerow(row)
    print(f"Data has been successfully written to {csv_file}.")

def append_to_history(current_csv, history_csv):
    """
    Appends current data to the historical record.
    
    Args:
        current_csv (str): Path to the current CSV data file
        history_csv (str): Path to the historical CSV file
    
    Returns:
        None. Appends current data to historical record.
    """
    file_exists = os.path.isfile(history_csv)

    with open(history_csv, 'a', newline='', encoding='utf-8') as history_file:
        with open(current_csv, 'r', newline='', encoding='utf-8') as current_file:
            reader = csv.reader(current_file)
            writer = csv.writer(history_file, quoting=csv.QUOTE_MINIMAL)

            # Read and write the header if history_csv doesn't exist
            header = next(reader)
            if not file_exists:
                writer.writerow(header)

            # Append rows with proper quoting
            for row in reader:
                writer.writerow(row)

    print(f"Data from {current_csv} has been appended to {history_csv}.")
    
def generate_changes_last_n(history_csv, changes_csv, n):
    """
    Analyzes and generates a report of changes in the last N versions of each study.
    
    Args:
        history_csv (str): Path to the historical data CSV file
        changes_csv (str): Path where the changes report will be saved
        n (int): Number of most recent versions to compare
    
    Returns:
        None. Generates a CSV file containing detected changes.
    """
    # Read historical data
    df = pd.read_csv(history_csv)

    # Ensure the Timestamp column is in datetime format
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

    # Normalize relevant date columns
    date_columns = ['StartDate', 'PrimaryCompletionDate', 'CompletionDate',
                    'StudyFirstPostDate', 'ResultsFirstPostDate', 'LastUpdatePostDate']
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
            except ValueError:
                df[col] = pd.to_datetime(df[col], errors='coerce')

    # Drop rows with null values in the Timestamp column
    df.dropna(subset=['Timestamp'], inplace=True)

    # Sort by NCTId and Timestamp
    df.sort_values(by=['NCTId', 'Timestamp'], inplace=True)

    changes = []

    # Identify latest NCTIds in the dataset
    latest_data = df[df['Timestamp'] == df['Timestamp'].max()]
    latest_nct_ids = set(latest_data['NCTId'])

    # Identify NCTIds in the previous dataset
    previous_data = df[df['Timestamp'] < df['Timestamp'].max()]
    previous_nct_ids = set(previous_data['NCTId'])

    # Identify new studies added (NCTIds that are in the latest data but not in the previous)
    new_nct_ids = latest_nct_ids - previous_nct_ids
    for nct_id in new_nct_ids:
        changes.append({
            'NCTId': nct_id,
            'final_date': df[df['NCTId'] == nct_id]['Timestamp'].max(),
            'start_date': None,
            'change': 'New Study Added',
            'final_value': None,
            'start_value': None
        })

    # Identify studies removed (NCTIds that are in the previous data but not in the latest)
    removed_nct_ids = previous_nct_ids - latest_nct_ids
    for nct_id in removed_nct_ids:
        changes.append({
            'NCTId': nct_id,
            'final_date': None,
            'start_date': df[df['NCTId'] == nct_id]['Timestamp'].max(),
            'change': 'Study Removed',
            'final_value': None,
            'start_value': None
        })

    # Group by NCTId and analyze the last N Timestamps
    for nct_id, group in df.groupby('NCTId'):
        if len(group) > 1:
            # Take only the last N records per NCTId
            group = group.tail(n).reset_index(drop=True)

            # Compare each row with the next
            for i in range(1, len(group)):
                current_row = group.iloc[i]
                previous_row = group.iloc[i - 1]

                for column in group.columns:
                    if column not in ['NCTId', 'Timestamp']:
                        current_value = current_row[column]
                        previous_value = previous_row[column]

                        # Handle numeric values
                        if pd.api.types.is_numeric_dtype(group[column]):
                            if pd.notnull(current_value) and pd.notnull(previous_value) and current_value != previous_value:
                                changes.append({
                                    'NCTId': nct_id,
                                    'final_date': current_row['Timestamp'],
                                    'start_date': previous_row['Timestamp'],
                                    'change': column,
                                    'final_value': current_value,
                                    'start_value': previous_value
                                })
                        # Handle date values
                        elif pd.api.types.is_datetime64_any_dtype(group[column]):
                            if pd.notnull(current_value) and pd.notnull(previous_value) and current_value != previous_value:
                                changes.append({
                                    'NCTId': nct_id,
                                    'final_date': current_row['Timestamp'],
                                    'start_date': previous_row['Timestamp'],
                                    'change': column,
                                    'final_value': current_value.strftime('%Y-%m-%d') if pd.notnull(current_value) else None,
                                    'start_value': previous_value.strftime('%Y-%m-%d') if pd.notnull(previous_value) else None
                                })

                        # Handle text or other types
                        else:
                            # Ignore double carriage return differences
                            if pd.notnull(current_value) and pd.notnull(previous_value):
                                current_value_str = str(current_value).replace('\n\n', '\n')
                                previous_value_str = str(previous_value).replace('\n\n', '\n')
                                if current_value_str != previous_value_str:
                                    changes.append({
                                        'NCTId': nct_id,
                                        'final_date': current_row['Timestamp'],
                                        'start_date': previous_row['Timestamp'],
                                        'change': column,
                                        'final_value': current_value_str,
                                        'start_value': previous_value_str
                                    })

    # Create DataFrame with changes
    changes_df = pd.DataFrame(changes)

    # Modify the 'change' column to be more readable
    def make_human_readable(text):
        return re.sub(r'(?<!^)(?=[A-Z])', ' ', text)

    changes_df['change'] = changes_df['change'].apply(make_human_readable)

    # Save changes to CSV
    changes_df.to_csv(changes_csv, index=False)

    print(f"Changes file generated at: {changes_csv}")

def clean_status(status):
    """
    Clean status field to make it more human friendly.
    Converts text like 'NOT_YET_RECRUITING' to 'Not Yet Recruiting'
    """
    if not status:
        return ""
    # Replace underscores with spaces and convert to title case
    words = status.replace('_', ' ').title()
    return words

if __name__ == "__main__":
    # Define file paths
    json_file = os.path.join('data', 'studies.json')
    csv_file = os.path.join('data', 'studies.csv')
    history_csv = os.path.join('data', 'studies_history.csv')
    
    # Execute ETL pipeline
    download_studies(100000)  # Download latest data
    data_preparation(json_file, csv_file)  # Transform data
    append_to_history(csv_file, history_csv)  # Update historical record
    generate_changes_last_n(history_csv, os.path.join('data', 'changes.csv'), 10)  # Generate change report
