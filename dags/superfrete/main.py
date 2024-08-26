import os
import logging
import pandas as pd
import numpy as np
from os import getenv
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
import gspread
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Configuração do logging
logging.basicConfig(level=logging.INFO)

# Função auxiliar para carregar YAML
def get_yaml(path):
    with open(path, 'r') as file:
        yaml_content = file.read()
    return yaml.safe_load(yaml_content)

# Função para manipulação do Google Sheets
def get_gsheet_data():
    gc = gspread.service_account(
        filename=f"{getenv('PYTHONPATH')}{getenv('CREDENTIALS_PATH')}"
    )
    sheets_info = get_yaml(f"{getenv('PYTHONPATH')}{getenv('SHEETS_PATH')}")
    sheet = sheets_info.get('retail_sales_dataset')
    sh = gc.open_by_key(sheet.get("url_key"))
    list_of_lists = sh.worksheet('retail_sales_dataset').get_all_values()
    df = pd.DataFrame(data=list_of_lists[1:], columns=list_of_lists[0], dtype=str)

    for column in sheet.get("columns"):
        column_type = column.get("type")
        column_name = column.get("name")
        date_format = column.get("format")

        if column_type == "float64":
            df[column_name] = df[column_name].apply(lambda x: str(x).replace(",", "."))
            df[column_name] = df[column_name].apply(lambda x: "0" if x == '' else x)
            df[column_name] = df[column_name].astype(column_type)

        elif column_type == "datetime":
            df[column_name] = df[column_name].apply(
                lambda x: (datetime.strptime(x, date_format)
                           if datetime.strptime(x, date_format) > datetime(2000, 1, 1)
                           else datetime(2000, 1, 1))
                if not pd.isnull(x)
                else x
            )
            df[column_name] = df[column_name].astype("datetime64[ns]")

        elif column_type == "int64":
            df[column_name] = df[column_name].replace({"": "0"})
            df[column_name] = df[column_name].astype(pd.Int64Dtype())

        elif column_type == "string":
            df[column_name] = df[column_name].replace({np.nan: None})

        else:
            df[column_name] = df[column_name].astype(column_type)

    return df, sheet.get("columns")

# Função para manipulação do BigQuery
def handle_bigquery(dataset, action, table_name=None, column_list=None, data=None):
    credentials = service_account.Credentials.from_service_account_file(
        f"{getenv('PYTHONPATH')}{getenv('CREDENTIALS_PATH')}",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials)
    dataset_id = dataset

    if action == "create_dataset":
        dataset = bigquery.Dataset(f"superfrete-433717.{dataset_id}")
        dataset.location = "us-east4"
        logging.info(f"Creating BigQuery Dataset {dataset_id}")
        client.create_dataset(dataset, timeout=30, exists_ok=True)

    elif action == "create_table":
        schema = [bigquery.SchemaField(column.get('name'), column.get('type'), mode="NULLABLE") for column in column_list]
        logging.info(f"Creating table in BigQuery {dataset_id}.{table_name}")
        table = bigquery.Table(f"superfrete-433717.{dataset_id}.{table_name}", schema=schema)
        client.create_table(table, exists_ok=True)

    elif action == "delete_table":
        table_id = f"{dataset_id}.{table_name}"
        logging.info(f"Deleting table in BigQuery {dataset_id}.{table_name}")
        client.delete_table(table_id, not_found_ok=True)

    elif action == "insert_data":
        schema = [bigquery.SchemaField(column.get('name'), column.get('type'), mode="NULLABLE") for column in column_list]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
        )
        logging.info(f"Inserting data into BigQuery {dataset_id}.{table_name}")
        client.load_table_from_dataframe(
            data, f"superfrete-433717.{dataset_id}.{table_name}", job_config=job_config
        )

# Função principal para executar o processo
def main():
    logging.info('Starting process')
    data, sheet = get_gsheet_data()
    handle_bigquery(dataset="superfrete_sales", action="insert_data",
                    table_name='retail_sales_dataset', column_list=sheet, data=data)

# Definição da DAG do Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    'sales',
    default_args=default_args,
    schedule_interval="0 */6 * * *",
    catchup=False,
) as dag:
    sales = PythonOperator(
        task_id='sales',
        python_callable=main,
    )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    main()
