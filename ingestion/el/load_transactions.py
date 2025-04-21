from ingestion.utils.load_raw import ingest_raw_data


def ingest_transactions():
    """
    Création de la table RAW.TRANSACTIONS et chargement des fichiers transactions_*.csv dans Snowflake.
    """
    # DDL
    ddl = (
        'transaction_id BIGINT,'
        'client_id BIGINT,'
        'date DATE,'
        'hour INT,'
        'minute INT,'
        'product_id BIGINT,'
        'quantity INT,'
        'store_id BIGINT'
    )
    
    return ingest_raw_data(
        table_name='transactions',
        columns_definition=ddl,
        file_pattern=r'transactions_.*\.csv'
    )
