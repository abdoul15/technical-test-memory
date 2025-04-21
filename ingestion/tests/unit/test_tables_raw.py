"""
Tests unitaires pour le module tables_raw.py
"""
import unittest
from unittest.mock import patch, MagicMock

from ingestion.utils.tables_raw import ensure_raw_schema, ensure_raw_table, load_raw_data, RAW_SCHEMA


class TestTablesRaw(unittest.TestCase):
    """Tests pour les fonctions du module tables_raw"""

    def test_raw_schema_constant(self):
        """Test que la constante RAW_SCHEMA est correctement définie"""
        from ingestion.utils.config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
        self.assertEqual(RAW_SCHEMA, f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")

    def test_ensure_raw_schema(self):
        """Test que ensure_raw_schema exécute la bonne requête SQL"""
        # Arrange
        mock_session = MagicMock()
        mock_sql_result = MagicMock()
        mock_session.sql.return_value = mock_sql_result
        
        # Act
        ensure_raw_schema(mock_session)
        
        # Assert
        mock_session.sql.assert_called_once()
        sql_query = mock_session.sql.call_args[0][0]
        self.assertIn(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}", sql_query)
        mock_sql_result.collect.assert_called_once()

    def test_ensure_raw_table(self):
        """Test que ensure_raw_table exécute les bonnes requêtes SQL"""
        # Arrange
        mock_session = MagicMock()
        mock_sql_result = MagicMock()
        mock_session.sql.return_value = mock_sql_result
        
        # Act
        ensure_raw_table(
            mock_session, 
            table_name="test_table", 
            columns_definition="id BIGINT, name VARCHAR"
        )
        
        # Assert
        self.assertEqual(mock_session.sql.call_count, 2) 
        create_table_call = mock_session.sql.call_args_list[1]
        sql_query = create_table_call[0][0]
        self.assertIn(f"CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.test_table", sql_query)
        self.assertIn("id BIGINT, name VARCHAR", sql_query)
        self.assertEqual(mock_sql_result.collect.call_count, 2)

    def test_load_raw_data(self):
        """Test que load_raw_data exécute la bonne requête SQL"""
        # Arrange
        mock_session = MagicMock()
        mock_sql_result = MagicMock()
        mock_session.sql.return_value = mock_sql_result
        
        # Act
        load_raw_data(
            mock_session, 
            table_name="test_table", 
            stage_name="test_stage", 
            pattern="test_*.csv",
            file_format="csv_format",
            on_error="CONTINUE"
        )
        
        # Assert
        mock_session.sql.assert_called_once()
        sql_query = mock_session.sql.call_args[0][0]
        self.assertIn(f"COPY INTO {RAW_SCHEMA}.test_table", sql_query)
        self.assertIn("FROM @test_stage", sql_query)
        self.assertIn("PATTERN = 'test_*.csv'", sql_query)
        self.assertIn("FILE_FORMAT = (FORMAT_NAME = 'csv_format')", sql_query)
        self.assertIn("ON_ERROR = 'CONTINUE'", sql_query)
        mock_sql_result.collect.assert_called_once()


if __name__ == '__main__':
    unittest.main()
