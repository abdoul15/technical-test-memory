"""
Tests d'intégration pour le flux d'ingestion complet
"""
import unittest
from unittest.mock import patch, MagicMock

from ingestion.main import main
from ingestion.el.load_clients import ingest_clients



class TestIngestionFlow(unittest.TestCase):
    """Tests d'intégration pour le flux d'ingestion complet"""

    @patch('ingestion.el.load_transactions.ingest_raw_data')
    @patch('ingestion.el.load_clients.ingest_raw_data')
    @patch('ingestion.el.load_products.ingest_raw_data')
    @patch('ingestion.el.load_stores.ingest_raw_data')
    def test_main_success(self, mock_stores, mock_products, mock_clients, mock_transactions):
        """Test que la fonction main exécute toutes les fonctions d'ingestion avec succès"""
        # Arrange
        mock_stores.return_value = True
        mock_products.return_value = True
        mock_clients.return_value = True
        mock_transactions.return_value = True
        
        # Act
        main()
        
        # Assert
        mock_stores.assert_called_once()
        mock_products.assert_called_once()
        mock_clients.assert_called_once()
        mock_transactions.assert_called_once()

    @patch('ingestion.el.load_transactions.ingest_raw_data')
    @patch('ingestion.el.load_clients.ingest_raw_data')
    @patch('ingestion.el.load_products.ingest_raw_data')
    @patch('ingestion.el.load_stores.ingest_raw_data')
    def test_main_partial_failure(self, mock_stores, mock_products, mock_clients, mock_transactions):
        """Test que la fonction main continue même si certaines ingestions échouent"""

        mock_stores.return_value = False  # Échec
        mock_products.return_value = True
        mock_clients.return_value = False  # Échec
        mock_transactions.return_value = True
        
        main()
        
        mock_stores.assert_called_once()
        mock_products.assert_called_once()
        mock_clients.assert_called_once()
        mock_transactions.assert_called_once()


class TestEndToEndFlow(unittest.TestCase):
    """Tests de bout en bout pour le flux d'ingestion et de transformation"""

    @patch('ingestion.utils.load_raw.Session')
    def test_end_to_end_flow(self, mock_session_class):
        """
        Test de bout en bout simulant le flux complet d'ingestion et de transformation
        
        Note: Ce test est plus complexe et nécessiterait un environnement de test dédié
        avec des données de test. Ici, nous nous contentons de mocker les appels externes.
        """
        # Arrange - Configuration des mocks
        mock_session = MagicMock()
        mock_builder = MagicMock()
        mock_configs = MagicMock()
        mock_configs.create.return_value = mock_session
        mock_builder.configs.return_value = mock_configs
        mock_session_class.builder = mock_builder
        
        mock_sql_result = MagicMock()
        mock_session.sql.return_value = mock_sql_result
        
        result = ingest_clients()
        
        self.assertTrue(result)
        


if __name__ == '__main__':
    unittest.main()
