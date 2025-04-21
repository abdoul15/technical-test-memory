"""
DAG d'Ingestion pour charger les données depuis Azure Blob Storage vers Snowflake.

Ce DAG orchestre l'ingestion des données clients, produits, magasins et transactions
depuis Azure Blob Storage vers les tables brutes de Snowflake.
"""

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from pendulum import datetime
from ingestion.el.load_clients import ingest_clients
from ingestion.el.load_products import ingest_products
from ingestion.el.load_stores import ingest_stores
from ingestion.el.load_transactions import ingest_transactions
from airflow.models.param import Param


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=None,  # Pas d'exécution automatique
    catchup=False,
    default_args={"owner": "data_engineer", "retries": 3},
    tags=["ingestion", "retail"],
    doc_md=__doc__,
    params={
        "run_transactions": Param(True, type="boolean", description="Whether to run transactions ingestion"),
        "run_clients": Param(True, type="boolean", description="Whether to run clients ingestion"),
        "run_products": Param(True, type="boolean", description="Whether to run products ingestion"),
        "run_stores": Param(True, type="boolean", description="Whether to run stores ingestion"),
    },
)
def retail_ingestion():
    """
    DAG pour ingérer les données retail depuis Azure Blob Storage vers Snowflake.
    """
    
    @task(task_id="ingest_clients")
    def task_ingest_clients(**context):
        """Ingérer les données clients depuis Azure Blob Storage vers Snowflake."""
        if context["params"]["run_clients"]:
            return ingest_clients()
        return True
    
    @task(task_id="ingest_products")
    def task_ingest_products(**context):
        """Ingérer les données produits depuis Azure Blob Storage vers Snowflake."""
        if context["params"]["run_products"]:
            return ingest_products()
        return True
    
    @task(task_id="ingest_stores")
    def task_ingest_stores(**context):
        """Ingérer les données magasins depuis Azure Blob Storage vers Snowflake."""
        if context["params"]["run_stores"]:
            return ingest_stores()
        return True
    
    @task(task_id="ingest_transactions")
    def task_ingest_transactions(**context):
        """Ingérer les données transactions depuis Azure Blob Storage vers Snowflake."""
        if context["params"]["run_transactions"]:
            return ingest_transactions()
        return True
    
    # Définir les dépendances entre tâches
    # Exécuter les tables de dimension en parallèle, puis les transactions
    clients_task = task_ingest_clients()
    products_task = task_ingest_products()
    stores_task = task_ingest_stores()
    transactions_task = task_ingest_transactions()
    
    # Définir les dépendances : charger les données de dimension avant les transactions
    [clients_task, products_task, stores_task] >> transactions_task


# Instantiate the DAG
retail_ingestion_dag = retail_ingestion()
