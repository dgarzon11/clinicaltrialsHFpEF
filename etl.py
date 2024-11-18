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
            "Acronym,"
            "OverallStatus,"
            "BriefSummary,"
            "HasResults,"
            "Condition,"
            "InterventionType,"
            "InterventionName,"
            "PrimaryOutcomeMeasure,"
            "SecondaryOutcomeMeasure,"
            "LeadSponsorName,"
            "CollaboratorName,"
            "Sex,"
            "MinimumAge,"
            "MaximumAge,"
            "StdAge,"
            "Phase,"
            "EnrollmentCount,"
            "LeadSponsorClass,"
            "StudyType,"
            "DesignPrimaryPurpose,"
            "OrgStudyId,"
            "SecondaryId,"
            "StartDate,"
            "PrimaryCompletionDate,"
            "CompletionDate,"
            "StudyFirstPostDate,"
            "ResultsFirstPostDate,"
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
        'NCTId',
        'BriefTitle',
        'Acronym',
        'OverallStatus',
        'BriefSummary',
        'HasResults',
        'Condition',
        'InterventionType',
        'InterventionName',
        'PrimaryOutcomeMeasure',
        'SecondaryOutcomeMeasure',
        'LeadSponsorName',
        'CollaboratorName',
        'Sex',
        'MinimumAge',
        'MaximumAge',
        'StdAge',
        'Phase',
        'EnrollmentCount',
        'LeadSponsorClass',
        'StudyType',
        'DesignPrimaryPurpose',
        'OrgStudyId',
        'SecondaryId',
        'StartDate',
        'PrimaryCompletionDate',
        'CompletionDate',
        'StudyFirstPostDate',
        'ResultsFirstPostDate',
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
            description = protocol.get('descriptionModule', {})

            nct_id = identification.get('nctId', '')
            brief_title = identification.get('briefTitle', '')
            acronym = identification.get('acronym', '')
            overall_status = status.get('overallStatus', '')
            brief_summary = description.get('briefSummary', '')
            has_results = description.get('hasResults', '')
            condition = ', '.join(conditions_module.get('conditions', []))
            intervention_type = ', '.join(design.get('interventionType', []))
            intervention_name = ', '.join(design.get('interventionName', []))
            primary_outcome_measure = ', '.join(design.get('primaryOutcomeMeasure', []))
            secondary_outcome_measure = ', '.join(design.get('secondaryOutcomeMeasure', []))
            lead_sponsor = sponsor.get('leadSponsor', {}).get('name', '')
            collaborator = ', '.join(sponsor.get('collaborator', []))
            sex = ', '.join(design.get('sex', []))
            minimum_age = design.get('minimumAge', '')
            maximum_age = design.get('maximumAge', '')
            std_age = design.get('stdAge', '')
            phase = ', '.join(design.get('phase', []))
            enrollment_count = design.get('enrollmentInfo', {}).get('count', '')
            lead_sponsor_class = sponsor.get('leadSponsor', {}).get('class', '')
            study_type = design.get('studyType', '')
            design_primary_purpose = design.get('designInfo', {}).get('primaryPurpose', '')
            org_study_id = identification.get('orgStudyIdInfo', {}).get('id', '')
            secondary_id = ', '.join([sid.get('id', '') for sid in identification.get('secondaryIdInfos', [])])
            start_date = status.get('startDateStruct', {}).get('date', '')
            primary_completion_date = status.get('primaryCompletionDateStruct', {}).get('date', '')
            completion_date = status.get('completionDateStruct', {}).get('date', '')
            study_first_post_date = status.get('studyFirstPostDateStruct', {}).get('date', '')
            results_first_post_date = status.get('resultsFirstPostDateStruct', {}).get('date', '')
            last_update_post_date = status.get('lastUpdatePostDateStruct', {}).get('date', '')

            writer.writerow({
                'NCTId': nct_id,
                'BriefTitle': brief_title,
                'Acronym': acronym,
                'OverallStatus': overall_status,
                'BriefSummary': brief_summary,
                'HasResults': has_results,
                'Condition': condition,
                'InterventionType': intervention_type,
                'InterventionName': intervention_name,
                'PrimaryOutcomeMeasure': primary_outcome_measure,
                'SecondaryOutcomeMeasure': secondary_outcome_measure,
                'LeadSponsorName': lead_sponsor,
                'CollaboratorName': collaborator,
                'Sex': sex,
                'MinimumAge': minimum_age,
                'MaximumAge': maximum_age,
                'StdAge': std_age,
                'Phase': phase,
                'EnrollmentCount': enrollment_count,
                'LeadSponsorClass': lead_sponsor_class,
                'StudyType': study_type,
                'DesignPrimaryPurpose': design_primary_purpose,
                'OrgStudyId': org_study_id,
                'SecondaryId': secondary_id,
                'StartDate': start_date,
                'PrimaryCompletionDate': primary_completion_date,
                'CompletionDate': completion_date,
                'StudyFirstPostDate': study_first_post_date,
                'ResultsFirstPostDate': results_first_post_date,
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
    json_file = os.path.join('data', 'dmd_current.json')
    csv_file = os.path.join('data', 'dmd_current.csv')
    history_csv = os.path.join('data', 'dmd_history.csv')

    json_to_csv(json_file, csv_file)
    append_to_history(csv_file, history_csv)



