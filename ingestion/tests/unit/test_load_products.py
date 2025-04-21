"""
Tests unitaires pour le module load_products.py
"""
import unittest
from unittest.mock import patch, MagicMock

from ingestion.el.load_products import ingest_products


class TestLoadProducts(unittest.TestCase):
    """Tests pour la fonction ingest_products"""

    @patch('ingestion.el.load_products.ingest_raw_data')
    def test_ingest_products_success(self, mock_ingest_raw_data):
        """Test que ingest_products retourne True quand ingest_raw_data réussit"""
        # Arrange
        mock_ingest_raw_data.return_value = True
        
        # Act
        result = ingest_products()
        
        # Assert
        self.assertTrue(result)
        mock_ingest_raw_data.assert_called_once()
        args, kwargs = mock_ingest_raw_data.call_args
        self.assertEqual(kwargs['table_name'], 'products')
        self.assertEqual(kwargs['file_pattern'], r'products\.csv')
        self.assertIn('id BIGINT', kwargs['columns_definition'])
        self.assertIn('ean VARCHAR', kwargs['columns_definition'])
        self.assertIn('brand VARCHAR', kwargs['columns_definition'])
        self.assertIn('description VARCHAR', kwargs['columns_definition'])

    @patch('ingestion.el.load_products.ingest_raw_data')
    def test_ingest_products_failure(self, mock_ingest_raw_data):
        """Test que ingest_products retourne False quand ingest_raw_data échoue"""
        # Arrange
        mock_ingest_raw_data.return_value = False
        
        # Act
        result = ingest_products()
        
        # Assert
        self.assertFalse(result)
        mock_ingest_raw_data.assert_called_once()


if __name__ == '__main__':
    unittest.main()
