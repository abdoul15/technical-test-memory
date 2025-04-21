"""
Configuration pour les tests pytest
"""
import os
import sys
import pytest
from unittest.mock import MagicMock

# Répertoire parent au chemin pour pouvoir importer les modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


@pytest.fixture
def mock_snowflake_session():
    """Fixture pour mocker une session Snowflake"""
    mock_session = MagicMock()
    mock_sql_result = MagicMock()
    mock_session.sql.return_value = mock_sql_result
    return mock_session


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Fixture pour mocker les variables d'environnement"""
    env_vars = {
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_PASSWORD': 'test_password',
        'SNOWFLAKE_ROLE': 'test_role',
        'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
        'SNOWFLAKE_DATABASE': 'test_database',
        'SNOWFLAKE_SCHEMA': 'test_schema',
        'AZURE_SAS_TOKEN': 'test_sas_token',
        'AZURE_STAGE_URL': 'https://test.blob.core.windows.net/container',
    }
    
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    
    return env_vars
