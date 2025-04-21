from ingestion.utils.load_raw import ingest_raw_data


def ingest_products():
    """
    Création de la table RAW.PRODUCTS et chargement du fichier products.csv dans Snowflake.
    """
    # Définition des colonnes pour la table PRODUCTS
    ddl = (
        'id BIGINT,'
        'ean VARCHAR,'
        'brand VARCHAR,'
        'description VARCHAR'
    )
    
    # Utilisation de la fonction générique pour l'ingestion
    return ingest_raw_data(
        table_name='products',
        columns_definition=ddl,
        file_pattern=r'products\.csv'
    )
