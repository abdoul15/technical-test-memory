"""
Tests unitaires pour le module load_clients.py
"""
import unittest
from unittest.mock import patch, MagicMock

from ingestion.el.load_clients import ingest_clients


class TestLoadClients(unittest.TestCase):
    """Tests pour la fonction ingest_clients"""

    @patch('ingestion.el.load_clients.ingest_raw_data')
    def test_ingest_clients_success(self, mock_ingest_raw_data):
        """Test que ingest_clients retourne True quand ingest_raw_data réussit"""
        # Arrange
        mock_ingest_raw_data.return_value = True
        
        # Act
        result = ingest_clients()
        
        # Assert
        self.assertTrue(result)
        mock_ingest_raw_data.assert_called_once()
        args, kwargs = mock_ingest_raw_data.call_args
        self.assertEqual(kwargs['table_name'], 'clients')
        self.assertEqual(kwargs['file_pattern'], r'clients\.csv')
        self.assertIn('id BIGINT', kwargs['columns_definition'])
        self.assertIn('name VARCHAR', kwargs['columns_definition'])
        self.assertIn('job VARCHAR', kwargs['columns_definition'])
        self.assertIn('email VARCHAR', kwargs['columns_definition'])
        self.assertIn('account_id VARCHAR', kwargs['columns_definition'])

    @patch('ingestion.el.load_clients.ingest_raw_data')
    def test_ingest_clients_failure(self, mock_ingest_raw_data):
        """Test que ingest_clients retourne False quand ingest_raw_data échoue"""
        # Arrange
        mock_ingest_raw_data.return_value = False
        
        # Act
        result = ingest_clients()
        
        # Assert
        self.assertFalse(result)
        mock_ingest_raw_data.assert_called_once()


if __name__ == '__main__':
    unittest.main()
