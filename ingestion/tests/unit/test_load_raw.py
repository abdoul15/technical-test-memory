"""
Tests unitaires pour le module load_raw.py
"""
import unittest
from unittest.mock import patch, MagicMock

from ingestion.utils.load_raw import ingest_raw_data


class TestLoadRaw(unittest.TestCase):
    """Tests pour la fonction ingest_raw_data"""

    @patch('ingestion.utils.load_raw.Session.builder')
    @patch('ingestion.utils.load_raw.ensure_file_format')
    @patch('ingestion.utils.load_raw.ensure_stage')
    @patch('ingestion.utils.load_raw.ensure_raw_table')
    @patch('ingestion.utils.load_raw.load_raw_data')
    def test_ingest_raw_data_success(self, mock_load_raw_data, mock_ensure_raw_table, 
                                    mock_ensure_stage, mock_ensure_file_format, mock_builder):
        """Test que ingest_raw_data retourne True quand tout se passe bien"""
        # Arrange
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        
        # Act
        result = ingest_raw_data(
            table_name='test_table',
            columns_definition='id BIGINT, name VARCHAR',
            file_pattern='test_*.csv'
        )
        
        # Assert
        self.assertTrue(result)
        mock_builder.configs.assert_called_once()
        mock_ensure_file_format.assert_called_once_with(mock_session, name='csv_format')
        mock_ensure_stage.assert_called_once()
        mock_ensure_raw_table.assert_called_once_with(
            mock_session, 
            table_name='test_table', 
            columns_definition='id BIGINT, name VARCHAR'
        )
        mock_load_raw_data.assert_called_once_with(
            mock_session, 
            table_name='test_table', 
            pattern='test_*.csv', 
            stage_name='raw_stage_sas',
            file_format='csv_format'
        )
        mock_session.close.assert_called_once()

    @patch('ingestion.utils.load_raw.Session.builder')
    @patch('ingestion.utils.load_raw.ensure_file_format')
    def test_ingest_raw_data_exception(self, mock_ensure_file_format, mock_builder):
        """Test que ingest_raw_data retourne False quand une exception est levée"""
        # Arrange
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_ensure_file_format.side_effect = Exception("Test exception")
        
        # Act
        result = ingest_raw_data(
            table_name='test_table',
            columns_definition='id BIGINT, name VARCHAR',
            file_pattern='test_*.csv'
        )
        
        # Assert
        self.assertFalse(result)
        mock_session.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
