import requests
import json

def main():
    # Definir la URL base
    BASE_URL = 'https://clinicaltrials.gov/api/query/study_fields'

    # Definir la expresión de búsqueda
    expr = 'Duchenne muscular dystrophy'

    # Definir los campos que queremos obtener
    fields = [
        'NCTId',
        'Title',
        'Condition',
        'StudyType',
        'EnrollmentCount',
        'OverallStatus',
        'LocationFacility',
        'BriefSummary',
        'Phase',
        'StudyResults',
        'StartDate',
        'CompletionDate',
        'PrimaryCompletionDate',
        'InterventionName',
        'InterventionType',
        'SponsorName',
        'Gender',
        'MinimumAge',
        'MaximumAge',
        'OutcomeMeasureTitle',
        'OutcomeMeasureType',
    ]

    # Unir los campos con comas
    fields_str = ','.join(fields)

    # Primera solicitud para obtener el número total de estudios
    params = {
        'expr': expr,
        'fields': fields_str,
        'min_rnk': 1,
        'max_rnk': 1,  # Solo necesitamos un resultado para obtener el conteo total
        'fmt': 'json'
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"La solicitud falló: {e}")
        return

    # Obtener el número total de estudios
    n_studies = int(data['StudyFieldsResponse']['NStudiesFound'])
    print(f"Número total de estudios encontrados: {n_studies}")

    # Ahora recuperar todos los estudios en lotes
    batch_size = 100  # Máximo permitido por solicitud
    all_studies = []
    for start in range(1, n_studies+1, batch_size):
        end = min(start + batch_size - 1, n_studies)
        print(f"Recuperando estudios del {start} al {end}")
        params['min_rnk'] = start
        params['max_rnk'] = end
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            studies = data['StudyFieldsResponse']['StudyFields']
            all_studies.extend(studies)
        except requests.exceptions.RequestException as e:
            print(f"La solicitud falló: {e}")
            continue
        except KeyError as e:
            print(f"Error de clave: {e}")
            continue

    print(f"Se recuperaron {len(all_studies)} estudios")

    # Opcionalmente, guardar en un archivo JSON
    with open('duchenne_trials.json', 'w', encoding='utf-8') as f:
        json.dump(all_studies, f, ensure_ascii=False, indent=2)

    print("Datos guardados en duchenne_trials.json")

if __name__ == "__main__":
    main()
