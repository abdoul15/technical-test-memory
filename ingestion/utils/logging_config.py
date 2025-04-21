"""
Configuration du logging pour le projet d'ingestion
"""
import logging
import os
import sys
from datetime import datetime
from pathlib import Path


class LoggingConfig:
    """Classe de configuration du logging pour le projet"""
    
    DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    
    @staticmethod
    def setup(log_level=logging.INFO, log_file=None, log_format=None, date_format=None):
        """
        Configure le logging pour le projet
        
        Args:
            log_level: Niveau de log (default: logging.INFO)
            log_file: Chemin vers le fichier de log (default: None, logs en console uniquement)
            log_format: Format des messages de log (default: DEFAULT_FORMAT)
            date_format: Format de la date dans les logs (default: DEFAULT_DATE_FORMAT)
        
        Returns:
            Logger racine configuré
        """
        # Créer le répertoire de logs si nécessaire
        if log_file:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                try:
                    os.makedirs(log_dir)
                except (PermissionError, OSError) as e:
                    print(f"Avertissement: Impossible de créer le répertoire de logs {log_dir}: {str(e)}")
                    # Utiliser un répertoire temporaire accessible
                    import tempfile
                    temp_dir = tempfile.gettempdir()
                    log_file = os.path.join(temp_dir, os.path.basename(log_file))
                    print(f"Utilisation du fichier de log alternatif: {log_file}")
        
        # Configuration de base
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Supprimer les handlers existants
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Format de log
        formatter = logging.Formatter(
            log_format or LoggingConfig.DEFAULT_FORMAT,
            date_format or LoggingConfig.DEFAULT_DATE_FORMAT
        )
        
        # Handler console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # Handler fichier si spécifié
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        
        # Désactiver la propagation des logs pour les bibliothèques externes
        for logger_name in ["snowflake", "azure"]:
            ext_logger = logging.getLogger(logger_name)
            ext_logger.propagate = False
        
        return root_logger
    
    @staticmethod
    def get_logger(name):
        """
        Obtient un logger configuré pour le module spécifié
        
        Args:
            name: Nom du module (généralement __name__)
        
        Returns:
            Logger configuré
        """
        return logging.getLogger(name)


# Initialisation par défaut
logger = LoggingConfig.setup(
    log_level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO")),
    log_file=os.environ.get("LOG_FILE")
)
