"""
Module principal pour l'ingestion des données dans Snowflake.
"""
from datetime import datetime
import logging
from ingestion.el.load_transactions import ingest_transactions
from ingestion.el.load_clients import ingest_clients
from ingestion.el.load_products import ingest_products
from ingestion.el.load_stores import ingest_stores
from ingestion.utils.logging_config import LoggingConfig

# Obtenir un logger pour ce module
logger = LoggingConfig.get_logger(__name__)


def main():
    """
    Exécute tous les processus d'ingestion.
    """
    # Date et heure d'exécution
    execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Démarrage de l'ingestion des données... [{execution_time}]")
    
    # Liste des tâches d'ingestion à exécuter
    ingestion_tasks = [
        {"name": "transactions", "function": ingest_transactions},
        {"name": "clients", "function": ingest_clients},
        {"name": "produits", "function": ingest_products},
        {"name": "magasins", "function": ingest_stores}
    ]
    
    # Exécution de chaque tâche d'ingestion
    success_count = 0
    for task in ingestion_tasks:
        start_time = datetime.now()
        logger.info(f"Ingestion des {task['name']}...")
        
        if task['function']():
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Ingestion des {task['name']} terminée en {duration:.2f} secondes")
            success_count += 1
        else:
            logger.error(f"Échec de l'ingestion des {task['name']}")
    
    # Bilan final
    if success_count == len(ingestion_tasks):
        logger.info("Tous les processus d'ingestion sont terminés avec succès!")
    else:
        logger.warning(f"{success_count}/{len(ingestion_tasks)} processus d'ingestion terminés avec succès.")


if __name__ == "__main__":
    main()
