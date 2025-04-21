import os
from dotenv import load_dotenv

load_dotenv()

# Configuration Snowflake
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Configuration Azure Blob Storage
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_ACCOUNT_NAME")
AZURE_STORAGE_CONTAINER = os.getenv("AZURE_CONTAINER_NAME")
AZURE_SAS_TOKEN = os.getenv("AZURE_SAS_TOKEN")
AZURE_STAGE_URL = f"azure://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{AZURE_STORAGE_CONTAINER}"

# Validation des variables d'environnement
required_vars = [
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
    "AZURE_ACCOUNT_NAME", "AZURE_CONTAINER_NAME", "AZURE_SAS_TOKEN"
]



missing = [v for v in required_vars if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

# Connexion (Snowpark session configuration)
SNOWPARK_CONNECTION_PARAMS = {
    "account": SNOWFLAKE_ACCOUNT,
    "user": SNOWFLAKE_USER,
    "password": SNOWFLAKE_PASSWORD,
    "role": SNOWFLAKE_ROLE,
    "warehouse": SNOWFLAKE_WAREHOUSE,
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_SCHEMA,
}
