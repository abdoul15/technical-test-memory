from snowflake.snowpark import Session
from ingestion.utils.config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA


RAW_SCHEMA = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}"


def ensure_raw_schema(session: Session):
    """
    Crée le schéma RAW s'il n'existe pas.
    """
    session.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};
    """).collect()


def ensure_raw_table(session: Session, table_name: str, columns_definition: str):
    """
    Crée la table RAW.<table_name> si elle n'existe pas.
    Assure d'abord que le schéma RAW existe.
    columns_definition: chaîne SQL listant les colonnes, ex: "id BIGINT, name VARCHAR"
    """
    # S'assurer que le schéma RAW existe
    ensure_raw_schema(session)
    
    session.sql(f"""
    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{table_name} (
      {columns_definition}
    );
    """).collect()


def load_raw_data(session: Session,
                  table_name: str,
                  stage_name: str,
                  pattern: str,
                  file_format: str = "csv_format",
                  on_error: str = "CONTINUE"):
    """
    Charge les fichiers du stage vers RAW.<table_name>.
    pattern: regex pour filtrer les blobs (ex: 'transactions_.*\\.csv').
    file_format: nom du FILE FORMAT à utiliser.
    on_error: comportement en cas d'erreur ('CONTINUE' ou 'ABORT_STATEMENT').
    """
    session.sql(f"""
    COPY INTO {RAW_SCHEMA}.{table_name}
      FROM @{stage_name}
      PATTERN = '{pattern}'
      FILE_FORMAT = (FORMAT_NAME = '{file_format}')
      ON_ERROR = '{on_error}'
    ;
    """).collect()
