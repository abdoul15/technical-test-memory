from snowflake.snowpark import Session
import logging
from ingestion.utils.config import SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, AZURE_SAS_TOKEN, AZURE_STAGE_URL
from ingestion.utils.file_formats import ensure_file_format, ensure_stage
from ingestion.utils.tables_raw import ensure_raw_table, load_raw_data
from ingestion.utils.logging_config import LoggingConfig

# Obtenir un logger pour ce module
logger = LoggingConfig.get_logger(__name__)


def ingest_raw_data(table_name, columns_definition, file_pattern):
    """
    Fonction générique pour ingérer des données dans une table RAW.
    
    Args:
        table_name (str): Nom de la table à créer/utiliser
        columns_definition (str): Définition des colonnes pour la table
        file_pattern (str): Pattern regex pour les fichiers à charger
    """
    # Initialiser la session Snowpark
    session = Session.builder.configs({
        'account': SNOWFLAKE_ACCOUNT,
        'user': SNOWFLAKE_USER,
        'password': SNOWFLAKE_PASSWORD,
        'role': SNOWFLAKE_ROLE,
        'warehouse': SNOWFLAKE_WAREHOUSE,
        'database': SNOWFLAKE_DATABASE,
        'schema': SNOWFLAKE_SCHEMA
    }).create()

    try:
        # 1. Garantir le file format et le stage
        ensure_file_format(session, name='csv_format')
        ensure_stage(session, name='raw_stage_sas', url=AZURE_STAGE_URL, sas_token=AZURE_SAS_TOKEN)

        # 2. Création de la table RAW si absente
        ensure_raw_table(session, table_name=table_name, columns_definition=columns_definition)

        # 3. Chargement des données brutes
        load_raw_data(session, table_name=table_name, pattern=file_pattern, stage_name='raw_stage_sas', file_format='csv_format')
        
        return True
    except Exception as e:
        logger.error(f"Erreur lors de l'ingestion des données {table_name}: {str(e)}")
        return False
    finally:
        # Fermer la session dans tous les cas
        session.close()
