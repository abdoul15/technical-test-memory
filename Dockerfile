FROM quay.io/astronomer/astro-runtime:12.8.0

# Copier le module ingestion
COPY ingestion /usr/local/airflow/ingestion

# Installer le module ingestion
RUN pip install -e /usr/local/airflow/ingestion
