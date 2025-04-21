from ingestion.utils.load_raw import ingest_raw_data


def ingest_stores():
    """
    Création de la table RAW.STORES et chargement du fichier stores.csv dans Snowflake.
    """
    # Définition des colonnes pour la table STORES
    ddl = (
        'id BIGINT,'
        'latlng VARCHAR,'
        'opening INT,'
        'closing INT,'
        'type INT'
    )
    
    # Utilisation de la fonction générique pour l'ingestion
    return ingest_raw_data(
        table_name='stores',
        columns_definition=ddl,
        file_pattern=r'stores\.csv'
    )
