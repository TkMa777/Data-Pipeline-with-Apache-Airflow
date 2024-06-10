import pandas as pd
from vacances_scolaires_france import SchoolHolidayDates
from datetime import datetime
import holidays

def fetch_holidays_and_vacations():
    current_year = datetime.now().year

    school_holidays = SchoolHolidayDates().holidays_for_year(current_year)
    school_holidays_list = [{
        'date': pd.to_datetime(date),
        'nom_vacances': details['nom_vacances'],
        'vacances_zone_a': details['vacances_zone_a'],
        'vacances_zone_b': details['vacances_zone_b'],
        'vacances_zone_c': details['vacances_zone_c'],
        'is_public_holiday': False
    } for date, details in school_holidays.items()]

    fr_holidays = holidays.France(years=current_year)
    holidays_list = [{
        'date': pd.to_datetime(date),
        'nom_vacances': name,
        'vacances_zone_a': False,
        'vacances_zone_b': False,
        'vacances_zone_c': False,
        'is_public_holiday': True
    } for date, name in fr_holidays.items()]

    combined_holidays_list = school_holidays_list + holidays_list
    df_holidays = pd.DataFrame(combined_holidays_list)
    return df_holidays


df_holidays = fetch_holidays_and_vacations()

# On teste avec sauvegarde dans le csv

print(df_holidays.head())
df_holidays.to_csv('/opt/airflow/data/holidays.csv', index=False)


print("Data saved to 'holidays.csv'")