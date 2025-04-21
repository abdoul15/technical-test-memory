from ingestion.utils.load_raw import ingest_raw_data


def ingest_stores():
    """
    Création de la table RAW.STORES et chargement du fichier stores.csv dans Snowflake.
    """
    # DDL
    ddl = (
        'id BIGINT,'
        'latlng VARCHAR,'
        'opening INT,'
        'closing INT,'
        'type INT'
    )
    
    return ingest_raw_data(
        table_name='stores',
        columns_definition=ddl,
        file_pattern=r'stores\.csv'
    )
