from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert
import smtplib
from email.message import EmailMessage



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['your_email_address'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task(task_id="send_mail")
def send_mail(subject, content):
    # Configuration de l'envoi de mail
    gmail_cfg ={
        "server": "smtp.gmail.com",
        "port": "465",
        "email": "your_email_address",  # A renseigner avant de lancer
        "pwd": "your_app_password"  # A renseigner avant de lancer
    }

    msg = EmailMessage()
    msg["to"] = gmail_cfg["email"]
    msg["from"] = gmail_cfg["email"]
    msg["Subject"] = subject
    msg.set_content(content)

    with smtplib.SMTP_SSL(gmail_cfg["server"], int(gmail_cfg["port"])) as smtp:
        smtp.login(gmail_cfg["email"], gmail_cfg["pwd"])
        smtp.send_message(msg)

def load_data_to_postgres(df, table_name, if_exists='replace'):
    pg_hook = PostgresHook(postgres_conn_id='POSTGRES_CONNEXION')
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"Data successfully loaded to table {table_name}")

def fetch_and_load_holidays():
    from fetch_holidays import fetch_holidays_and_vacations
    df_holidays = fetch_holidays_and_vacations()
    load_data_to_postgres(df_holidays, 'holidays', if_exists='replace')

def fetch_and_load_temperatures():
    from fetch_data import fetch_temperatures
    pg_hook = PostgresHook(postgres_conn_id='POSTGRES_CONNEXION')
    engine = pg_hook.get_sqlalchemy_engine()
    table_name = 'temperatures'
    inspector = inspect(engine)

    df_temperatures = fetch_temperatures(max_iterations=15)

    if not inspector.has_table(table_name):
        load_data_to_postgres(df_temperatures, table_name)
    else:
        with engine.connect() as conn:
            last_timestamp = pd.read_sql(f"SELECT MAX(timestamp) FROM {table_name}", conn).iloc[0, 0]
            if last_timestamp:
                new_data = df_temperatures[df_temperatures['timestamp'] > last_timestamp]
                load_data_to_postgres(new_data, table_name, if_exists='append')
            else:
                load_data_to_postgres(df_temperatures, table_name)

def fetch_and_load_coefficients():
    from fetch_data import fetch_coefficients
    pg_hook = PostgresHook(postgres_conn_id='POSTGRES_CONNEXION')
    engine = pg_hook.get_sqlalchemy_engine()
    table_name = 'coefficients'
    inspector = inspect(engine)

    df_coefficients = fetch_coefficients(max_iterations=15)

    if not inspector.has_table(table_name):
        load_data_to_postgres(df_coefficients, table_name)
    else:
        with engine.connect() as conn:
            last_timestamp = pd.read_sql(f"SELECT MAX(timestamp) FROM {table_name}", conn).iloc[0, 0]
            if last_timestamp:
                from_date = (pd.to_datetime(last_timestamp) - timedelta(days=7)).strftime('%Y-%m-%d')
                new_data = df_coefficients[df_coefficients['timestamp'] >= from_date]

                stmt = insert(table_name).values(new_data.to_dict(orient='records'))
                update_dict = {c: stmt.excluded[c] for c in new_data.columns}
                stmt = stmt.on_conflict_do_update(index_elements=['timestamp'], set_=update_dict)

                conn.execute(stmt)
            else:
                load_data_to_postgres(df_coefficients, table_name)

def transform_and_load_data():
    from transform_data import transform_and_load_data as transform
    transform()

def drop_tables_if_needed(full_refresh=False):
    if full_refresh:
        pg_hook = PostgresHook(postgres_conn_id='POSTGRES_CONNEXION')
        engine = pg_hook.get_sqlalchemy_engine()
        conn = engine.connect()
        for table in ['temperatures', 'coefficients', 'holidays']:
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        conn.close()

with DAG(dag_id='data_pipeline', default_args=default_args, schedule_interval='0 8 * * *') as dag:
    task_full_refresh = PythonOperator(
        task_id='full_refresh',
        python_callable=drop_tables_if_needed,
        op_kwargs={'full_refresh': True}
    )

    task_fetch_holidays = PythonOperator(
        task_id='fetch_holidays',
        python_callable=fetch_and_load_holidays
    )

    task_fetch_temperatures = PythonOperator(
        task_id='fetch_temperatures',
        python_callable=fetch_and_load_temperatures
    )

    task_fetch_coefficients = PythonOperator(
        task_id='fetch_coefficients',
        python_callable=fetch_and_load_coefficients
    )

    task_transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_and_load_data
    )

    task_send_email = send_mail("Airflow DAG", "Execution avec succès")

    task_full_refresh >> [task_fetch_holidays, task_fetch_temperatures, task_fetch_coefficients] >> task_transform_data >> task_send_email
