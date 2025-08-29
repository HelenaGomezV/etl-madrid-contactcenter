from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from task_clean_contact import task_clean_contact
from task_clean_delitos import task_clean_delitos
from task_clean_renta import task_clean_renta

# Usa tu clase del loader
from task_load_raw import RawParquetLoader


def load_raw_all():
    RawParquetLoader().run(only="all")


def load_raw_one(source: str):
    RawParquetLoader().run(only=source)


def clean_renta_data():
    return task_clean_renta()


def clean_delitos_data():
    return task_clean_delitos()


def clean_contact_data():
    return task_clean_contact()


with DAG(
    dag_id="etl_general",
    description="ETL general call center madrid.",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Ejecuta manualmente
    catchup=False,
    default_args={"owner": "data-eng", "retries": 0},
    params={  # Permite elegir qué cargar al hacer Trigger
        "source": Param("all", enum=["all", "renta", "delitos", "contact"])
    },
    tags=["etl", "raw"],
) as dag:

    # Tarea única que usa el parámetro 'source'
    def _load_router(**context):
        source = context["params"]["source"]
        if source == "all":
            return load_raw_all()
        return load_raw_one(source)

    load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=_load_router,
        provide_context=True,
    )

    clean_renta = PythonOperator(
        task_id="clean_renta",
        python_callable=clean_renta_data,
    )

    clean_delitos = PythonOperator(
        task_id="clean_delitos",
        python_callable=clean_delitos_data,
    )

    clean_contact = PythonOperator(
        task_id="clean_contact",
        python_callable=clean_contact_data,
    )

    load_raw >> clean_renta >> clean_delitos >> clean_contact
