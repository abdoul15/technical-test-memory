from ingestion.utils.load_raw import ingest_raw_data


def ingest_clients():
    """
    Création de la table RAW.CLIENTS et chargement du fichier clients.csv dans Snowflake.
    """
    # DDL table CLIENTS
    ddl = (
        'id BIGINT,'
        'name VARCHAR,'
        'job VARCHAR,'
        'email VARCHAR,'
        'account_id VARCHAR'
    )
    
    # Fonction générique pour l'ingestion
    return ingest_raw_data(
        table_name='clients',
        columns_definition=ddl,
        file_pattern=r'clients\.csv'
    )
