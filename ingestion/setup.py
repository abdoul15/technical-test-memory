from setuptools import setup, find_packages

setup(
    name="ingestion",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
        "azure-storage-blob",
        "snowflake-connector-python",
        "snowflake-snowpark-python>=1.27.0",
    ],
)
