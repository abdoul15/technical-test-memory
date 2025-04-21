"""
DAG du Pipeline de Données Retail Complet.

Ce DAG orchestre l'ensemble du pipeline de données retail, comprenant :
1. L'ingestion des données depuis Azure Blob Storage vers Snowflake
2. La transformation des données brutes en modèles staging
3. La transformation des modèles staging en tables de dimensions et de faits
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from pendulum import datetime
from ingestion.el.load_clients import ingest_clients
from ingestion.el.load_products import ingest_products
from ingestion.el.load_stores import ingest_stores
from ingestion.el.load_transactions import ingest_transactions
import os


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="0 10 * * *",  # Exécution quotidienne à 10h00
    catchup=False,
    default_args={"owner": "data_engineer", "retries": 3},
    tags=["pipeline", "retail", "ingestion", "transform"],
    doc_md=__doc__,
)
def retail_pipeline():
    """
    DAG pour exécuter le pipeline complet de données retail.
    """
    
    # Définir le chemin vers le projet dbt
    # Utiliser un chemin relatif pour plus de portabilité
    import os
    from pathlib import Path
    
    # Obtenir le chemin absolu du répertoire des DAGs
    dags_dir = os.path.dirname(os.path.abspath(__file__))
    # Remonter d'un niveau pour obtenir le répertoire racine du projet
    project_root = os.path.dirname(dags_dir)
    # Chemin vers le projet dbt
    DBT_PROJECT_DIR = os.path.join(project_root, "dbt", "retail_transform")
    
    # Tâches d'ingestion
    @task(task_id="ingest_clients")
    def task_ingest_clients():
        """Ingérer les données clients depuis Azure Blob Storage vers Snowflake."""
        return ingest_clients()
    
    @task(task_id="ingest_products")
    def task_ingest_products():
        """Ingérer les données produits depuis Azure Blob Storage vers Snowflake."""
        return ingest_products()
    
    @task(task_id="ingest_stores")
    def task_ingest_stores():
        """Ingérer les données magasins depuis Azure Blob Storage vers Snowflake."""
        return ingest_stores()
    
    @task(task_id="ingest_transactions")
    def task_ingest_transactions():
        """Ingérer les données transactions depuis Azure Blob Storage vers Snowflake."""
        return ingest_transactions()
    
    # Tâches DBT
    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt clean",
    )
    
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && rm -f logs/dbt.log && dbt debug --no-write-json",
    )
    
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --no-write-json",
    )
    
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --no-write-json",
    )
    
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select staging --no-write-json",
    )
    
    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select core --no-write-json",
    )
    
    dbt_test_core = BashOperator(
        task_id="dbt_test_core",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select core --no-write-json",
    )
    
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate",
    )
    
    # Créer les instances de tâches
    clients_task = task_ingest_clients()
    products_task = task_ingest_products()
    stores_task = task_ingest_stores()
    transactions_task = task_ingest_transactions()
    
    # Définir les dépendances entre tâches
    
    # Dépendances d'ingestion : charger les données de dimension avant les transactions
    [clients_task, products_task, stores_task] >> transactions_task
    
    # Connecter l'ingestion à la transformation
    transactions_task >> dbt_clean >> dbt_debug >> dbt_deps
    
    # Dépendances de transformation
    dbt_deps >> dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> dbt_run_core >> dbt_test_core
    dbt_test_core >> dbt_docs_generate


# Instancier le DAG
retail_pipeline_dag = retail_pipeline()
