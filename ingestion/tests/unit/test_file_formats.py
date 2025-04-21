"""
Tests unitaires pour le module file_formats.py
"""
import unittest
from unittest.mock import patch, MagicMock

from ingestion.utils.file_formats import ensure_file_format, ensure_stage


class TestFileFormats(unittest.TestCase):
    """Tests pour les fonctions du module file_formats"""

    def test_ensure_file_format(self):
        """Test que ensure_file_format exécute la bonne requête SQL"""
        # Arrange
        mock_session = MagicMock()
        mock_sql_result = MagicMock()
        mock_session.sql.return_value = mock_sql_result
        
        # Act
        ensure_file_format(mock_session, name="test_format")
        
        # Assert
        mock_session.sql.assert_called_once()
        sql_query = mock_session.sql.call_args[0][0]
        from ingestion.utils.config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
        self.assertIn(f"CREATE FILE FORMAT IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.test_format", sql_query)
        self.assertIn("TYPE", sql_query)
        self.assertIn("FIELD_DELIMITER", sql_query)
        self.assertIn("SKIP_HEADER", sql_query)
        mock_sql_result.collect.assert_called_once()

    def test_ensure_file_format_default_name(self):
        """Test que ensure_file_format utilise le nom par défaut si non spécifié"""
        # Arrange
        mock_session = MagicMock()
        mock_sql_result = MagicMock()
        mock_session.sql.return_value = mock_sql_result
        
        # Act
        ensure_file_format(mock_session)  
        
        # Assert
        mock_session.sql.assert_called_once()
        sql_query = mock_session.sql.call_args[0][0]
        from ingestion.utils.config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
        self.assertIn(f"CREATE FILE FORMAT IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.csv_format", sql_query)

    def test_ensure_stage(self):
        """Test que ensure_stage exécute la bonne requête SQL"""
        # Arrange
        mock_session = MagicMock()
        mock_sql_result = MagicMock()
        mock_session.sql.return_value = mock_sql_result
        
        # Act
        ensure_stage(
            mock_session, 
            name="test_stage", 
            url="https://test.blob.core.windows.net/container", 
            sas_token="test_token",
            file_format="test_format"
        )
        
        # Assert
        mock_session.sql.assert_called_once()
        sql_query = mock_session.sql.call_args[0][0]
        from ingestion.utils.config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
        self.assertIn(f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.test_stage", sql_query)
        self.assertIn("URL = 'https://test.blob.core.windows.net/container'", sql_query)
        self.assertIn("CREDENTIALS = (AZURE_SAS_TOKEN = 'test_token')", sql_query)
        self.assertIn("FILE_FORMAT = test_format", sql_query)
        mock_sql_result.collect.assert_called_once()

    def test_ensure_stage_missing_params(self):
        """Test que ensure_stage lève une exception si url ou sas_token manquent"""
        # Arrange
        mock_session = MagicMock()
        
        # Act & Assert
        with self.assertRaises(ValueError):
            ensure_stage(mock_session, name="test_stage") 
        
        with self.assertRaises(ValueError):
            ensure_stage(mock_session, name="test_stage", url="https://test.com") 
        
        with self.assertRaises(ValueError):
            ensure_stage(mock_session, name="test_stage", sas_token="token") 


if __name__ == '__main__':
    unittest.main()
