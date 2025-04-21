"""
DAG de Transformation DBT pour les données retail.

Ce DAG orchestre les transformations dbt pour le pipeline de données retail,
transformant les données brutes en modèles staging puis en tables de dimensions et de faits.
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime
import os


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=None,  # Pas d'exécution automatique
    catchup=False,
    default_args={"owner": "data_engineer", "retries": 3},
    tags=["transform", "dbt", "retail"],
    doc_md=__doc__,
)
def retail_transform():
    """
    DAG pour transformer les données retail en utilisant dbt.
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
    
    # Exécuter dbt debug pour s'assurer que tout est correctement configuré
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && rm -f logs/dbt.log && dbt debug --no-write-json",
    )
    
    # Exécuter dbt deps pour installer les dépendances
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --no-write-json",
    )
    
    # Exécuter dbt run pour les modèles staging
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --no-write-json",
    )
    
    # Exécuter dbt test pour les modèles staging
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select staging --no-write-json",
    )
    
    # Exécuter dbt run pour les modèles core
    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select core --no-write-json",
    )
    
    # Exécuter dbt test pour les modèles core
    dbt_test_core = BashOperator(
        task_id="dbt_test_core",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select core --no-write-json",
    )
    
    # Générer la documentation dbt
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --no-write-json",
    )
    
    # Définir les dépendances entre tâches
    dbt_debug >> dbt_deps
    dbt_deps >> dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> dbt_run_core >> dbt_test_core
    dbt_test_core >> dbt_docs_generate


# Instantiate the DAG
retail_transform_dag = retail_transform()
