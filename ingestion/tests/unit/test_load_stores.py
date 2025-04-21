"""
Tests unitaires pour le module load_stores.py
"""
import unittest
from unittest.mock import patch, MagicMock

from ingestion.el.load_stores import ingest_stores


class TestLoadStores(unittest.TestCase):
    """Tests pour la fonction ingest_stores"""

    @patch('ingestion.el.load_stores.ingest_raw_data')
    def test_ingest_stores_success(self, mock_ingest_raw_data):
        """Test que ingest_stores retourne True quand ingest_raw_data réussit"""
        # Arrange
        mock_ingest_raw_data.return_value = True
        
        # Act
        result = ingest_stores()
        
        # Assert
        self.assertTrue(result)
        mock_ingest_raw_data.assert_called_once()
        args, kwargs = mock_ingest_raw_data.call_args
        self.assertEqual(kwargs['table_name'], 'stores')
        self.assertEqual(kwargs['file_pattern'], r'stores\.csv')
        self.assertIn('id BIGINT', kwargs['columns_definition'])
        self.assertIn('latlng VARCHAR', kwargs['columns_definition'])
        self.assertIn('opening INT', kwargs['columns_definition'])
        self.assertIn('closing INT', kwargs['columns_definition'])
        self.assertIn('type INT', kwargs['columns_definition'])

    @patch('ingestion.el.load_stores.ingest_raw_data')
    def test_ingest_stores_failure(self, mock_ingest_raw_data):
        """Test que ingest_stores retourne False quand ingest_raw_data échoue"""
        # Arrange
        mock_ingest_raw_data.return_value = False
        
        # Act
        result = ingest_stores()
        
        # Assert
        self.assertFalse(result)
        mock_ingest_raw_data.assert_called_once()


if __name__ == '__main__':
    unittest.main()
