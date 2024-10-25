import json
import csv
import datetime
import os  # Import os to check if the history file exists

def json_to_csv(json_file, csv_file):
    # Open the JSON file and load the data
    with open(json_file, 'r') as f:
        data = json.load(f)

    # Define the CSV columns, adding 'Timestamp' to the list
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
        'Timestamp'  # New column for timestamp
    ]

    # Open the CSV file for writing
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()

        # Get the current timestamp once
        timestamp = datetime.datetime.now().isoformat()

        # Iterate over each study in the JSON data
        for study in data:
            protocol = study.get('protocolSection', {})
            identification = protocol.get('identificationModule', {})
            status = protocol.get('statusModule', {})
            sponsor = protocol.get('sponsorCollaboratorsModule', {})
            conditions_module = protocol.get('conditionsModule', {})
            design = protocol.get('designModule', {})

            # Extract fields
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

            # Write the row to CSV
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
                'Timestamp': timestamp  # Include the timestamp
            })

    print(f"Data has been successfully written to {csv_file}.")

def append_to_history(current_csv, history_csv):
    # Check if the history file exists
    file_exists = os.path.isfile(history_csv)

    # Open the history file in append mode
    with open(history_csv, 'a', newline='', encoding='utf-8') as history_file:
        # Open the current CSV file
        with open(current_csv, 'r', newline='', encoding='utf-8') as current_file:
            reader = csv.reader(current_file)
            writer = csv.writer(history_file)

            # Read the header from the current file
            header = next(reader)

            # If the history file does not exist, write the header
            if not file_exists:
                writer.writerow(header)

            # Write the rest of the rows to the history file
            for row in reader:
                writer.writerow(row)

    print(f"Data from {current_csv} has been appended to {history_csv}.")

if __name__ == "__main__":
    json_file = 'duchenne_studies_sample.json'  # Replace with your JSON file name
    csv_file = 'duchenne_studies_sample.csv'    # Desired CSV output file name
    history_csv = 'duchenne_studies_history.csv'  # CSV history file name

    # Convert JSON to CSV
    json_to_csv(json_file, csv_file)

    # Append current CSV to history CSV
    append_to_history(csv_file, history_csv)


