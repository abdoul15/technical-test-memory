"""Tests for the ingestion DAG."""

import pytest
from airflow.models import DagBag
from airflow.utils.task_group import TaskGroup


@pytest.fixture
def dagbag():
    return DagBag(include_examples=False)


def test_ingestion_dag_loaded(dagbag):
    """Test that the ingestion DAG is loaded correctly."""
    dag = dagbag.get_dag("retail_ingestion")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) > 0


def test_ingestion_dag_structure(dagbag):
    """Test the structure of the ingestion DAG."""
    dag = dagbag.get_dag("retail_ingestion")
    
    # Verify tasks exist
    task_ids = [task.task_id for task in dag.tasks]
    expected_tasks = ["ingest_clients", "ingest_products", "ingest_stores", "ingest_transactions"]
    for task_id in expected_tasks:
        assert task_id in task_ids
    
    # Verify dependencies
    upstream_tasks = {
        "ingest_transactions": ["ingest_clients", "ingest_products", "ingest_stores"]
    }
    
    for task_id, upstream_task_ids in upstream_tasks.items():
        task = dag.get_task(task_id)
        upstream_task_ids_set = {t.task_id for t in task.upstream_list}
        assert set(upstream_task_ids) == upstream_task_ids_set


def test_ingestion_dag_default_args(dagbag):
    """Test the default arguments of the ingestion DAG."""
    dag = dagbag.get_dag("retail_ingestion")
    
    assert dag.default_args["owner"] == "data_engineer"
    assert dag.default_args["retries"] == 3
    assert dag.schedule_interval is None  # No automatic scheduling
    assert dag.catchup is False
    assert "ingestion" in dag.tags
    assert "retail" in dag.tags
