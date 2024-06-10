from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

def transform_and_load_data():
    pg_hook = PostgresHook(postgres_conn_id='POSTGRES_CONNEXION')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    query = """
    SELECT 
        t.timestamp,
        t.trl,
        t.tnl,
        c.sous_profil,
        c.cp,
        EXTRACT(DOW FROM t.timestamp) as day_of_week,
        EXTRACT(DOY FROM t.timestamp) as day_of_year,
        (EXTRACT(HOUR FROM t.timestamp) * 2 + EXTRACT(MINUTE FROM t.timestamp) / 30) as half_hour,
        CONCAT(
            CASE WHEN h.vacances_zone_a THEN '1' ELSE '0' END,
            CASE WHEN h.vacances_zone_b THEN '1' ELSE '0' END,
            CASE WHEN h.vacances_zone_c THEN '1' ELSE '0' END
        ) as fr_holiday,
        CASE WHEN h.is_public_holiday THEN TRUE ELSE FALSE END as is_public_holiday
    FROM 
        temperatures t
    LEFT JOIN 
        coefficients c ON t.timestamp = c.timestamp
    LEFT JOIN 
        holidays h ON t.timestamp = h.date
    """

    cursor.execute(query)
    result = cursor.fetchall()

    columns = [
        'timestamp', 'trl', 'tnl', 'sous_profil', 'cp',
        'day_of_week', 'day_of_year', 'half_hour', 'fr_holiday', 'is_public_holiday'
    ]
    df = pd.DataFrame(result, columns=columns)

    load_data_to_postgres(df, 'data_model_inputs_6')

    cursor.close()
    conn.close()

def load_data_to_postgres(df, table_name, schema=None):
    pg_hook = PostgresHook(postgres_conn_id='POSTGRES_CONNEXION')
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)
