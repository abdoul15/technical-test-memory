from snowflake.snowpark import Session
from ingestion.utils.config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA


def ensure_file_format(session: Session, name: str = "csv_format"):
    """
    Crée un File Format robuste pour parser les CSV transactions, produits, clients, stores.
    """
    sql = f"""
    CREATE FILE FORMAT IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{name}
      TYPE                         = 'CSV'
      FIELD_DELIMITER              = ';'
      SKIP_HEADER                  = 1
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
      ESCAPE_UNENCLOSED_FIELD      = '\\\\'
      TRIM_SPACE                   = TRUE
      ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
      SKIP_BLANK_LINES             = TRUE;
    """
    session.sql(sql).collect()


def ensure_stage(session: Session,
                 name: str = "raw_stage_sas",
                 url: str = None,
                 sas_token: str = None,
                 file_format: str = "csv_format"):
    """
    Crée un Stage Azure si non existant, avec SAS token et file_format par défaut.
    """
    if url is None or sas_token is None:
        raise ValueError("URL et SAS_TOKEN doivent être spécifiés pour créer le Stage")
    
    sql = f"""
    CREATE STAGE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{name}
      URL = '{url}'
      CREDENTIALS = (AZURE_SAS_TOKEN = '{sas_token}')
      FILE_FORMAT = {file_format};
    """
    session.sql(sql).collect()
