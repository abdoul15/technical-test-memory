
CONTAINER_WEBSERVER = $(shell docker ps --format '{{.Names}}' | grep -E 'webserver')
DBT_DIR = /usr/local/airflow/dbt/retail_transform

.PHONY: start stop restart status bash

start:
	@echo "Démarrage de l'environnement Astro..."
	astro dev start

stop:
	@echo "Arrêt de l'environnement Astro..."
	astro dev stop

restart:
	@echo "Redémarrage de l'environnement Astro..."
	astro dev restart

status:
	@echo "Statut de l'environnement Astro..."
	astro dev ps

bash:
	@echo "Connexion au conteneur webserver..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) bash

# Commandes pour l'ingestion
.PHONY: ingestion test test-unit test-integration test-coverage test-dags install-test-deps

install-test-deps:
	@echo "Installation des dépendances de test..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) pip install pytest-cov

ingestion:
	@echo "Exécution de l'ingestion via main.py..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) python -W ignore::DeprecationWarning /usr/local/airflow/ingestion/main.py

# Exécution de tous les tests
test:
	@echo "Exécution de tous les tests..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) python -W ignore::DeprecationWarning -m pytest -xvs /usr/local/airflow/ingestion/tests/

test-unit:
	@echo "Exécution des tests unitaires..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) python -W ignore::DeprecationWarning -m pytest -xvs /usr/local/airflow/ingestion/tests/unit/

test-integration:
	@echo "Exécution des tests d'intégration..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) python -W ignore::DeprecationWarning -m pytest -xvs /usr/local/airflow/ingestion/tests/integration/

test-coverage: install-test-deps
	@echo "Exécution des tests avec couverture de code..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) python -W ignore::DeprecationWarning -m pytest --cov=ingestion /usr/local/airflow/ingestion/tests/ --cov-report=term-missing

# Exécution des tests des DAGs
test-dags: install-test-deps
	@echo "Exécution des tests des DAGs..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) python -W ignore::DeprecationWarning -m pytest -xvs /usr/local/airflow/tests/dags/

# Commandes pour dbt
.PHONY: dbt-clean dbt-deps dbt-run-staging dbt-test-staging dbt-run-core dbt-test-core dbt-transform dbt-display-tables

dbt-clean:
	@echo "Nettoyage des fichiers dbt..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) bash -c "cd $(DBT_DIR) && rm -rf target || true && mkdir -p target || true"

dbt-deps:
	@echo "Installation des dépendances dbt..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) bash -c "cd $(DBT_DIR) && dbt deps"

dbt-run-staging:
	@echo "Exécution des modèles staging..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) bash -c "cd $(DBT_DIR) && dbt run --select staging"

dbt-test-staging:
	@echo "Test des modèles staging..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) bash -c "cd $(DBT_DIR) && dbt test --select staging"

dbt-run-core:
	@echo "Exécution des modèles core..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) bash -c "cd $(DBT_DIR) && dbt run --select core"

dbt-test-core:
	@echo "Test des modèles core..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) bash -c "cd $(DBT_DIR) && dbt test --select core"

dbt-display-tables:
	@echo "Affichage des 10 premières lignes de chaque table finale..."
	@echo "Conteneur détecté: $(CONTAINER_WEBSERVER)"
	docker exec -it $(CONTAINER_WEBSERVER) bash -c "cd $(DBT_DIR) && dbt run-operation display_final_tables"

dbt-transform: dbt-clean dbt-deps dbt-run-staging dbt-test-staging dbt-run-core dbt-test-core dbt-display-tables
	@echo "Toutes les commandes dbt ont été exécutées"

.PHONY: pipeline

pipeline: ingestion dbt-transform
	@echo "Pipeline complet exécuté"

.PHONY: help

help:
	@echo "Makefile pour le projet Retail Pipeline"
	@echo ""
	@echo "Commandes disponibles :"
	@echo "  make start              - Démarrer l'environnement Astro"
	@echo "  make stop               - Arrêter l'environnement Astro"
	@echo "  make restart            - Redémarrer l'environnement Astro"
	@echo "  make status             - Afficher le statut de l'environnement Astro"
	@echo "  make bash               - Se connecter au conteneur webserver"
	@echo "  make ingestion          - Exécuter l'ingestion via main.py"
	@echo "  make install-test-deps  - Installer les dépendances de test"
	@echo "  make test               - Exécuter tous les tests"
	@echo "  make test-unit          - Exécuter les tests unitaires"
	@echo "  make test-integration   - Exécuter les tests d'intégration"
	@echo "  make test-coverage      - Exécuter les tests avec couverture de code"
	@echo "  make test-dags          - Exécuter les tests des DAGs"
	@echo "  make dbt-clean          - Nettoyer les fichiers dbt"
	@echo "  make dbt-deps           - Installer les dépendances dbt"
	@echo "  make dbt-run-staging    - Exécuter les modèles staging"
	@echo "  make dbt-test-staging   - Tester les modèles staging"
	@echo "  make dbt-run-core       - Exécuter les modèles core"
	@echo "  make dbt-test-core      - Tester les modèles core"
	@echo "  make dbt-display-tables - Afficher les 10 premières lignes de chaque table finale"
	@echo "  make dbt-transform      - Exécuter toutes les transformations dbt"
	@echo "  make pipeline           - Exécuter le pipeline complet (ingestion + dbt)"
	@echo "  make help               - Afficher cette aide"

.DEFAULT_GOAL := help
