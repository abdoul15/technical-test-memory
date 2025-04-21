from ingestion.utils.load_raw import ingest_raw_data


def ingest_products():
    """
    Création de la table RAW.PRODUCTS et chargement du fichier products.csv dans Snowflake.
    """
    ddl = (
        'id BIGINT,'
        'ean VARCHAR,'
        'brand VARCHAR,'
        'description VARCHAR'
    )
    
    return ingest_raw_data(
        table_name='products',
        columns_definition=ddl,
        file_pattern=r'products\.csv'
    )
