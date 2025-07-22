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

def clean_unicode_text(text):
    """
    Clean and normalize Unicode text by properly handling special characters.
    
    Args:
        text (str): Text that may contain Unicode characters
    
    Returns:
        str: Properly decoded text with normalized characters
    """
    if not isinstance(text, str):
        return ''
    
    # Handle HTML entities and common Unicode issues
    import html
    try:
        # First unescape any HTML entities
        text = html.unescape(text)
        
        # Handle direct Unicode characters
        text = text.encode('utf-8', errors='ignore').decode('utf-8')
        
        # Normalize to composed form (combining characters are merged)
        import unicodedata
        text = unicodedata.normalize('NFKC', text)
        
        return text.strip()
    except Exception as e:
        print(f"Error cleaning text: {e}")
        return text

def download_studies(page_size):
    """
    Downloads clinical trials data from clinicaltrials.gov API.
    Searches for studies related to both 'duchenne' and 'DMD' conditions.
    
    Args:
        page_size (int): Number of studies to retrieve
    
    Returns:
        None. Saves downloaded data to a JSON file in the data directory.
    """
    # Using OR operator to search for either HFpEF or 'HFpEF - Heart Failure With Preserved Ejection Fraction' in conditions
    base_url = "https://clinicaltrials.gov/api/v2/studies?query.cond=HFpEF%20-%20Heart%20Failure%20With%20Preserved%20Ejection%20Fraction"
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
        print(f"Step 1: Successfully downloaded and saved {len(studies)} studies to {file_name}.")

    except requests.RequestException as e:
        print(f"Error in Step 1: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")

def parse_age_to_months(age_str: str) -> float:
    """
    Convert an age string (e.g. '2 Years', '3 Months', '1 Week', '1 Day')
    into the number of months (float). Approximations:
        1 Day  = 1/30 Month
        1 Week = 1/4 Month
        1 Month = 1 Month
        1 Year  = 12 Months
    """
    if not age_str or not isinstance(age_str, str):
        return 0.0
        
    # Clean up the string
    age_str = age_str.strip().lower()
    
    # Use a regex to capture the numeric part of the string (including decimals if any)
    match = re.match(r'(\d+(?:\.\d+)?)', age_str)
    if not match:
        # If there's no numeric match, default to 0
        return 0.0
    
    # Convert the captured numeric part to float
    quantity = float(match.group(1))
    
    # Determine the unit by keywords
    if "year" in age_str:
        return quantity * 12
    elif "month" in age_str:
        return quantity
    elif "week" in age_str:
        return quantity / 4
    elif "day" in age_str:
        return quantity / 30
    else:
        # If the unit is unrecognized, return 0
        return 0.0

def data_preparation(json_file, csv_file):
    """
    Transforms JSON clinical trials data into a structured CSV format and creates separate files for conditions and locations.
    
    Args:
        json_file (str): Path to the source JSON file
        csv_file (str): Path where the main CSV file will be saved
    
    Returns:
        None. Writes processed data to specified CSV files.
    """
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    csv_columns = [
        'NCTId', 'BriefTitle', 'Acronym', 'OverallStatus', 'BriefSummary',
        'HasResults', 'Condition', 'InterventionType', 'InterventionName',
        'PrimaryOutcomeMeasure', 'SecondaryOutcomeMeasure', 'LeadSponsorName',
        'CollaboratorName', 'Sex', 'MinimumAge', 'MaximumAge', 'MinimumAgeMonths', 
        'MaximumAgeMonths', 'StdAge', 'Phase', 'EnrollmentCount', 'LeadSponsorClass', 
        'StudyType', 'DesignPrimaryPurpose', 'OrgStudyId', 'SecondaryId', 'StartDate', 
        'PrimaryCompletionDate', 'CompletionDate', 'StudyFirstPostDate', 
        'ResultsFirstPostDate', 'LastUpdatePostDate', 'Timestamp'
    ]

    # Create lists to store condition, location, and intervention mappings
    conditions_data = []
    locations_data = []
    interventions_data = []
    sponsors_collaborators_data = []  # New list for sponsors and collaborators

    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns, extrasaction='ignore')
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
            contacts_locations = protocol.get('contactsLocationsModule', {})

            # Process interventions for the main CSV file
            intervention_types = [format_title_case(i.get('type', '')) for i in interventions]
            intervention_names = [i.get('name', '') for i in interventions]

            row = {
                'NCTId': identification.get('nctId', ''),
                'BriefTitle': clean_unicode_text(identification.get('briefTitle', '')),
                'Acronym': clean_unicode_text(identification.get('acronym', '')),
                'OverallStatus': format_title_case(status.get('overallStatus', '')),
                'BriefSummary': clean_unicode_text(description.get('briefSummary', '')),
                'HasResults': study.get('hasResults', ''),
                'Condition': clean_unicode_text(', '.join(conditions.get('conditions', []))),
                'InterventionType': clean_unicode_text(', '.join(intervention_types)),
                'InterventionName': clean_unicode_text(', '.join(intervention_names)),
                'PrimaryOutcomeMeasure': clean_unicode_text(', '.join(o.get('measure', '') for o in outcomes.get('primaryOutcomes', []))),
                'SecondaryOutcomeMeasure': clean_unicode_text(', '.join(o.get('measure', '') for o in outcomes.get('secondaryOutcomes', []))),
                'LeadSponsorName': clean_unicode_text(sponsor.get('leadSponsor', {}).get('name', '')),
                'CollaboratorName': clean_unicode_text(', '.join(c.get('name', '') for c in sponsor.get('collaborators', []))),
                'Sex': format_title_case(eligibility.get('sex', '')),
                'MinimumAge': eligibility.get('minimumAge', ''),
                'MaximumAge': eligibility.get('maximumAge', ''),
                'MinimumAgeMonths': parse_age_to_months(eligibility.get('minimumAge', '')),
                'MaximumAgeMonths': parse_age_to_months(eligibility.get('maximumAge', '')),
                'StdAge': ', '.join(format_title_case(age) for age in eligibility.get('stdAges', [])),
                'Phase': ', '.join(format_title_case(phase) for phase in design.get('phases', [])),
                'EnrollmentCount': design.get('enrollmentInfo', {}).get('count', ''),
                'LeadSponsorClass': format_title_case(sponsor.get('leadSponsor', {}).get('class', '')),
                'StudyType': format_title_case(design.get('studyType', '')),
                'DesignPrimaryPurpose': format_title_case(design.get('designInfo', {}).get('primaryPurpose', '')),
                'OrgStudyId': identification.get('orgStudyIdInfo', {}).get('id', ''),
                'SecondaryId': clean_unicode_text(', '.join(sid.get('id', '') for sid in identification.get('secondaryIdInfos', []))),
                'StartDate': standardize_date(status.get('startDateStruct', {}).get('date', '')),
                'PrimaryCompletionDate': standardize_date(status.get('primaryCompletionDateStruct', {}).get('date', '')),
                'CompletionDate': standardize_date(status.get('completionDateStruct', {}).get('date', '')),
                'StudyFirstPostDate': standardize_date(status.get('studyFirstPostDateStruct', {}).get('date', '')),
                'ResultsFirstPostDate': standardize_date(status.get('resultsFirstPostDateStruct', {}).get('date', '')),
                'LastUpdatePostDate': standardize_date(status.get('lastUpdatePostDateStruct', {}).get('date', '')),
                'Timestamp': timestamp
            }
            writer.writerow(row)

            # Process conditions for the separate conditions file
            nct_id = identification.get('nctId', '')
            study_conditions = conditions.get('conditions', [])
            for condition in study_conditions:
                conditions_data.append({
                    'NCTId': nct_id,
                    'condition': condition.strip()
                })

            # Process locations for the separate locations file
            locations = contacts_locations.get('locations', [])
            for location in locations:
                locations_data.append({
                    'NCTId': nct_id,
                    'facility': clean_unicode_text(location.get('facility', '')),
                    'city': clean_unicode_text(location.get('city', '')),
                    'state': clean_unicode_text(location.get('state', '')),
                    'country': clean_unicode_text(location.get('country', '')),
                    'zip': clean_unicode_text(location.get('zip', '')),
                    'status': format_title_case(location.get('status', '')),
                    'recruitment_status': format_title_case(location.get('recruitmentStatus', ''))
                })

            # Process interventions for the separate interventions file
            for intervention in interventions:
                interventions_data.append({
                    'NCTId': nct_id,
                    'type': format_title_case(intervention.get('type', '')),
                    'name': clean_unicode_text(intervention.get('name', '')),
                    'description': clean_unicode_text(intervention.get('description', '')),
                    'arm_group_labels': clean_unicode_text(', '.join(intervention.get('armGroupLabels', []))),
                    'other_names': clean_unicode_text(', '.join(intervention.get('otherNames', [])))
                })

            # Process sponsors and collaborators for the separate sponsors file
            lead_sponsor = sponsor.get('leadSponsor', {})
            collaborators = sponsor.get('collaborators', [])
            
            if lead_sponsor:
                if collaborators:
                    # If there are collaborators, create a row for each collaborator
                    for collaborator in collaborators:
                        sponsors_collaborators_data.append({
                            'NCTId': nct_id,
                            'Sponsor': clean_unicode_text(lead_sponsor.get('name', '')),
                            'SponsorClass': format_title_case(lead_sponsor.get('class', '')),
                            'Collaborator': clean_unicode_text(collaborator.get('name', '')),
                            'CollaboratorClass': format_title_case(collaborator.get('class', ''))
                        })
                else:
                    # If there are no collaborators, create a single row with empty collaborator fields
                    sponsors_collaborators_data.append({
                        'NCTId': nct_id,
                        'Sponsor': clean_unicode_text(lead_sponsor.get('name', '')),
                        'SponsorClass': format_title_case(lead_sponsor.get('class', '')),
                        'Collaborator': '',
                        'CollaboratorClass': ''
                    })

    print(f"Step 2: Main data has been successfully written to {csv_file}.")

    # Write conditions to a separate CSV file
    conditions_file = os.path.join(os.path.dirname(csv_file), 'conditions.csv')
    conditions_df = pd.DataFrame(conditions_data)
    conditions_df.to_csv(conditions_file, index=False, encoding='utf-8-sig')
    print(f"Step 3: Conditions data has been successfully written to {conditions_file}.")

    # Write locations to a separate CSV file
    locations_file = os.path.join(os.path.dirname(csv_file), 'locations.csv')
    locations_df = pd.DataFrame(locations_data)
    locations_df.to_csv(locations_file, index=False, encoding='utf-8-sig')
    print(f"Step 4: Locations data has been successfully written to {locations_file}.")

    # Write interventions to a separate CSV file
    if interventions_data:  # Only create and save if we have intervention data
        interventions_df = pd.DataFrame(interventions_data)
        interventions_file = os.path.join(os.path.dirname(csv_file), 'interventions.csv')
        interventions_df.to_csv(interventions_file, index=False, encoding='utf-8-sig')
        print(f"Step 5: Interventions data has been successfully written to {interventions_file}.")

    # Write sponsors and collaborators to a separate CSV file
    sponsors_file = os.path.join(os.path.dirname(csv_file), 'sponsors_collaborators.csv')
    sponsors_df = pd.DataFrame(sponsors_collaborators_data)
    sponsors_df.to_csv(sponsors_file, index=False, encoding='utf-8-sig')
    print(f"Step 6: Sponsors and collaborators data has been successfully written to {sponsors_file}.")

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

    print(f"Step 6: Data from {current_csv} has been appended to {history_csv}.")
    
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
    try:
        # Read historical data with error handling for inconsistent columns
        df = pd.read_csv(history_csv, on_bad_lines='skip', encoding='utf-8-sig')
        print(f"Successfully read {len(df)} rows from history file")
        
        # Ensure the Timestamp column is in datetime format
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

        # Normalize relevant date columns
        date_columns = ['StartDate', 'PrimaryCompletionDate', 'CompletionDate',
                        'StudyFirstPostDate', 'ResultsFirstPostDate', 'LastUpdatePostDate']
        for col in date_columns:
            if (col in df.columns):
                try:
                    df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
                except ValueError:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        # Drop rows with null values in the Timestamp column
        df.dropna(subset=['Timestamp'], inplace=True)

        # Sort by NCTId and Timestamp
        df.sort_values(by=['NCTId', 'Timestamp'], inplace=True)

        changes = []

        # Initialize changes list with required columns
        changes_template = {
            'NCTId': None,
            'final_date': None,
            'start_date': None,
            'field_changed': None,  # Changed from 'change' to 'field_changed'
            'final_value': None,
            'start_value': None
        }

        # Identify latest NCTIds in the dataset
        latest_data = df[df['Timestamp'] == df['Timestamp'].max()]
        latest_nct_ids = set(latest_data['NCTId'])

        # Identify NCTIds in the previous dataset
        previous_data = df[df['Timestamp'] < df['Timestamp'].max()]
        previous_nct_ids = set(previous_data['NCTId'])

        # Identify new studies added (NCTIds that are in the latest data but not in the previous)
        new_nct_ids = latest_nct_ids - previous_nct_ids
        for nct_id in new_nct_ids:
            change_entry = changes_template.copy()
            change_entry.update({
                'NCTId': nct_id,
                'final_date': df[df['NCTId'] == nct_id]['Timestamp'].max(),
                'field_changed': 'New Study Added'
            })
            changes.append(change_entry)

        # Identify studies removed (NCTIds that are in the previous data but not in the latest)
        removed_nct_ids = previous_nct_ids - latest_nct_ids
        for nct_id in removed_nct_ids:
            change_entry = changes_template.copy()
            change_entry.update({
                'NCTId': nct_id,
                'start_date': df[df['NCTId'] == nct_id]['Timestamp'].max(),
                'field_changed': 'Study Removed'
            })
            changes.append(change_entry)

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
                                    change_entry = changes_template.copy()
                                    change_entry.update({
                                        'NCTId': nct_id,
                                        'final_date': current_row['Timestamp'],
                                        'start_date': previous_row['Timestamp'],
                                        'field_changed': column,
                                        'final_value': current_value,
                                        'start_value': previous_value
                                    })
                                    changes.append(change_entry)
                            # Handle date values
                            elif pd.api.types.is_datetime64_any_dtype(group[column]):
                                if pd.notnull(current_value) and pd.notnull(previous_value) and current_value != previous_value:
                                    change_entry = changes_template.copy()
                                    change_entry.update({
                                        'NCTId': nct_id,
                                        'final_date': current_row['Timestamp'],
                                        'start_date': previous_row['Timestamp'],
                                        'field_changed': column,
                                        'final_value': current_value.strftime('%Y-%m-%d') if pd.notnull(current_value) else None,
                                        'start_value': previous_value.strftime('%Y-%m-%d') if pd.notnull(previous_value) else None
                                    })
                                    changes.append(change_entry)
                            # Handle text or other types
                            else:
                                # Ignore double carriage return differences
                                if pd.notnull(current_value) and pd.notnull(previous_value):
                                    current_value_str = str(current_value).replace('\n\n', '\n')
                                    previous_value_str = str(previous_value).replace('\n\n', '\n')
                                    if current_value_str != previous_value_str:
                                        change_entry = changes_template.copy()
                                        change_entry.update({
                                            'NCTId': nct_id,
                                            'final_date': current_row['Timestamp'],
                                            'start_date': previous_row['Timestamp'],
                                            'field_changed': column,
                                            'final_value': current_value_str,
                                            'start_value': previous_value_str
                                        })
                                        changes.append(change_entry)

        # Create DataFrame with changes, handling empty changes list
        if not changes:
            changes = [changes_template]  # Add a dummy row to prevent empty DataFrame issues
        
        changes_df = pd.DataFrame(changes)

        # Modify the 'field_changed' column to be more readable
        def make_human_readable(text):
            if pd.isna(text):
                return "No changes detected"
            return re.sub(r'(?<!^)(?=[A-Z])', ' ', text)

        changes_df['field_changed'] = changes_df['field_changed'].apply(make_human_readable)

        # Save changes to CSV
        changes_df.to_csv(changes_csv, index=False)

        if len(changes) == 1 and all(v is None for v in changes[0].values()):
            print("Step 7: No changes detected in the data.")
        else:
            print(f"Step 7: Changes file generated at: {changes_csv}")
    except Exception as e:
        print(f"Error in Step 7: {e}")

def standardize_date(date_str):
    """
    Standardize date strings to YYYY-MM-DD format.
    
    Args:
        date_str (str): Date string to standardize
    
    Returns:
        str: Standardized date in YYYY-MM-DD format or empty string if invalid
    """
    if isinstance(date_str, str):
        try:
            if len(date_str.split('-')) == 2:  # Year-Month format
                date_str += '-01'
            return pd.to_datetime(date_str, errors='coerce').strftime('%Y-%m-%d')
        except Exception:
            return ''
    return ''

def format_title_case(text):
    """
    Format text in title case by replacing underscores with spaces.
    For example, converts 'NOT_YET_RECRUITING' to 'Not Yet Recruiting'
    
    Args:
        text (str): Text to format
    
    Returns:
        str: Text formatted in title case
    """
    if not text:
        return ""
    # Replace underscores with spaces and convert to title case
    words = text.replace('_', ' ').title()
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
