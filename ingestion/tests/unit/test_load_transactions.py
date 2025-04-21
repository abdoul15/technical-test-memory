"""
Tests unitaires pour le module load_transactions.py
"""
import unittest
from unittest.mock import patch, MagicMock

from ingestion.el.load_transactions import ingest_transactions


class TestLoadTransactions(unittest.TestCase):
    """Tests pour la fonction ingest_transactions"""

    @patch('ingestion.el.load_transactions.ingest_raw_data')
    def test_ingest_transactions_success(self, mock_ingest_raw_data):
        """Test que ingest_transactions retourne True quand ingest_raw_data réussit"""
        # Arrange
        mock_ingest_raw_data.return_value = True
        
        # Act
        result = ingest_transactions()
        
        # Assert
        self.assertTrue(result)
        mock_ingest_raw_data.assert_called_once()
        args, kwargs = mock_ingest_raw_data.call_args
        self.assertEqual(kwargs['table_name'], 'transactions')
        self.assertEqual(kwargs['file_pattern'], r'transactions_.*\.csv')
        self.assertIn('transaction_id BIGINT', kwargs['columns_definition'])
        self.assertIn('client_id BIGINT', kwargs['columns_definition'])
        self.assertIn('date DATE', kwargs['columns_definition'])
        self.assertIn('hour INT', kwargs['columns_definition'])
        self.assertIn('minute INT', kwargs['columns_definition'])
        self.assertIn('product_id BIGINT', kwargs['columns_definition'])
        self.assertIn('quantity INT', kwargs['columns_definition'])
        self.assertIn('store_id BIGINT', kwargs['columns_definition'])

    @patch('ingestion.el.load_transactions.ingest_raw_data')
    def test_ingest_transactions_failure(self, mock_ingest_raw_data):
        """Test que ingest_transactions retourne False quand ingest_raw_data échoue"""
        # Arrange
        mock_ingest_raw_data.return_value = False
        
        # Act
        result = ingest_transactions()
        
        # Assert
        self.assertFalse(result)
        mock_ingest_raw_data.assert_called_once()


if __name__ == '__main__':
    unittest.main()
