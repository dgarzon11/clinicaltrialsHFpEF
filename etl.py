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
    file_exists = os.path.isfile(history_csv)

    with open(history_csv, 'a', newline='', encoding='utf-8') as history_file:
        with open(current_csv, 'r', newline='', encoding='utf-8') as current_file:
            reader = csv.reader(current_file)
            writer = csv.writer(history_file)

            header = next(reader)  # Skip header from current file
            if not file_exists:
                writer.writerow(header)  # Write header only if the file doesn't exist

            for row in reader:
                writer.writerow(row)

    print(f"Data from {current_csv} has been appended to {history_csv}.")
    
def generate_changes_last_n(history_csv, changes_csv, n):
    # Leer el archivo de historial
    df = pd.read_csv(history_csv)

    # Asegurarse de que la columna Timestamp está en formato datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

    # Normalizar las columnas de fechas relevantes
    date_columns = ['StartDate', 'PrimaryCompletionDate', 'CompletionDate',
                    'StudyFirstPostDate', 'ResultsFirstPostDate', 'LastUpdatePostDate']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Eliminar filas con valores nulos en la columna Timestamp
    df.dropna(subset=['Timestamp'], inplace=True)

    # Ordenar por NCTId y Timestamp
    df.sort_values(by=['NCTId', 'Timestamp'], inplace=True)

    changes = []

    # Agrupar por NCTId y analizar los últimos 4 Timestamps
    for nct_id, group in df.groupby('NCTId'):
        if len(group) > 1:
            # Tomar solo los últimos 4 registros por NCTId
            group = group.tail(4).reset_index(drop=True)

            # Comparar cada fila con la siguiente
            for i in range(1, len(group)):
                current_row = group.iloc[i]
                previous_row = group.iloc[i - 1]

                for column in group.columns:
                    if column not in ['NCTId', 'Timestamp']:
                        current_value = current_row[column]
                        previous_value = previous_row[column]

                        # Manejar valores numéricos
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
                        # Manejar valores de fecha
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
                        # Manejar texto u otros tipos
                        else:
                            if pd.notnull(current_value) and pd.notnull(previous_value) and str(current_value) != str(previous_value):
                                changes.append({
                                    'NCTId': nct_id,
                                    'final_date': current_row['Timestamp'],
                                    'start_date': previous_row['Timestamp'],
                                    'change': column,
                                    'final_value': current_value,
                                    'start_value': previous_value
                                })

    # Crear un DataFrame con los cambios
    changes_df = pd.DataFrame(changes)

    # Guardar los cambios en un archivo CSV
    changes_df.to_csv(changes_csv, index=False)

    print(f"Archivo de cambios generado en: {changes_csv}")


if __name__ == "__main__":
    #download_studies(100000)
    #json_file = os.path.join('data', 'dmd_current.json')
    #csv_file = os.path.join('data', 'dmd_current.csv')
    history_csv = os.path.join('data', 'dmd_history.csv')

    #json_to_csv(json_file, csv_file)
    #append_to_history(csv_file, history_csv)
    generate_changes_last_n(history_csv, os.path.join('data', 'dmd_changes.csv'),10)
