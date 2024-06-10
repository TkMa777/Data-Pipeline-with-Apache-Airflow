import requests
import pandas as pd

def fetch_temperatures(max_iterations=10):
    temperatures = []
    offset = 0
    limit = 100
    iterations = 0

    while iterations < max_iterations:
        url_temperatures = (
            f"https://data.enedis.fr/api/explore/v2.1/catalog/datasets/"
            f"donnees-de-temperature-et-de-pseudo-rayonnement/records?limit={limit}&offset={offset}&refine=horodate%3A%222024%22"
        )
        response = requests.get(url_temperatures, timeout=10)
        data = response.json()

        records = data.get('results', [])
        if not records:
            break

        temperatures.extend([{
            'timestamp': record.get('horodate', None),
            'trl': record.get('temperature_realisee_lissee_degc', None),
            'tnl': record.get('temperature_normale_lissee_degc', None)
        } for record in records])

        offset += limit
        iterations += 1

    df = pd.DataFrame(temperatures)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.drop_duplicates(subset=['timestamp'])
    return df

def fetch_coefficients(max_iterations=10):
    coefficients = []
    offset = 0
    limit = 100
    iterations = 0

    while iterations < max_iterations:
        url_coefficients = (
            f"https://data.enedis.fr/api/explore/v2.1/catalog/datasets/"
            f"coefficients-des-profils/records?limit={limit}&offset={offset}&refine=horodate%3A%222024%22"
        )
        response = requests.get(url_coefficients, timeout=10)
        data = response.json()

        records = data.get('results', [])
        if not records:
            break

        coefficients.extend([{
            'timestamp': record.get('horodate', None),
            'sous_profil': record.get('sous_profil', None),
            'cp': record.get('coefficient_ajuste', None)
        } for record in records])

        offset += limit
        iterations += 1

    df = pd.DataFrame(coefficients)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.drop_duplicates(subset=['timestamp'])
    return df


# On teste avec sauvegarde dans le csv

df_temperatures = fetch_temperatures(max_iterations=15)
df_coefficients = fetch_coefficients(max_iterations=15)

print("Filtered temperatures data:")
print(df_temperatures.head())

print("Filtered coefficients data:")
print(df_coefficients.head())

df_temperatures.to_csv('/opt/airflow/data/temp.csv', index=False)
df_coefficients.to_csv('/opt/airflow/data/ceof.csv', index=False)

print("Data saved to 'temperatures.csv' and 'coefficients.csv'")
