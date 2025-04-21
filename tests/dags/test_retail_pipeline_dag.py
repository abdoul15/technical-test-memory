"""Tests for the retail pipeline DAG."""

import pytest
from airflow.models import DagBag


@pytest.fixture
def dagbag():
    return DagBag(include_examples=False)


def test_retail_pipeline_dag_loaded(dagbag):
    """Test that the retail pipeline DAG is loaded correctly."""
    dag = dagbag.get_dag("retail_pipeline")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) > 0


def test_retail_pipeline_dag_structure(dagbag):
    """Test the structure of the retail pipeline DAG."""
    dag = dagbag.get_dag("retail_pipeline")
    
    # Verify tasks exist
    task_ids = [task.task_id for task in dag.tasks]
    expected_tasks = [
        "ingest_clients", "ingest_products", "ingest_stores", "ingest_transactions",
        "dbt_clean", "dbt_debug", "dbt_deps", "dbt_run_staging", "dbt_test_staging", 
        "dbt_run_core", "dbt_test_core", "dbt_docs_generate"
    ]
    for task_id in expected_tasks:
        assert task_id in task_ids
    
    # Verify key dependencies
    # Ingestion dependencies
    ingestion_dependencies = {
        "ingest_transactions": ["ingest_clients", "ingest_products", "ingest_stores"]
    }
    
    for task_id, upstream_task_ids in ingestion_dependencies.items():
        task = dag.get_task(task_id)
        upstream_task_ids_set = {t.task_id for t in task.upstream_list}
        assert set(upstream_task_ids).issubset(upstream_task_ids_set)
    
    # Transformation dependencies
    transform_dependencies = {
        "dbt_clean": ["ingest_transactions"],
        "dbt_debug": ["dbt_clean"],
        "dbt_deps": ["dbt_debug"],
        "dbt_run_staging": ["dbt_deps"],
        "dbt_test_staging": ["dbt_run_staging"],
        "dbt_run_core": ["dbt_test_staging"],
        "dbt_test_core": ["dbt_run_core"],
        "dbt_docs_generate": ["dbt_test_core"]
    }
    
    for task_id, upstream_task_ids in transform_dependencies.items():
        task = dag.get_task(task_id)
        upstream_task_ids_set = {t.task_id for t in task.upstream_list}
        assert set(upstream_task_ids).issubset(upstream_task_ids_set)


def test_retail_pipeline_dag_default_args(dagbag):
    """Test the default arguments of the retail pipeline DAG."""
    dag = dagbag.get_dag("retail_pipeline")
    
    assert dag.default_args["owner"] == "data_engineer"
    assert dag.default_args["retries"] == 3
    assert dag.schedule_interval == "0 10 * * *"  # Daily at 10:00
    assert dag.catchup is False
    assert "pipeline" in dag.tags
    assert "retail" in dag.tags
    assert "ingestion" in dag.tags
    assert "transform" in dag.tags
