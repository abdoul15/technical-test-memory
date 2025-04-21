"""Tests for the dbt transformation DAG."""

import pytest
from airflow.models import DagBag


@pytest.fixture
def dagbag():
    return DagBag(include_examples=False)


def test_dbt_transform_dag_loaded(dagbag):
    """Test that the dbt transformation DAG is loaded correctly."""
    dag = dagbag.get_dag("retail_transform")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) > 0


def test_dbt_transform_dag_structure(dagbag):
    """Test the structure of the dbt transformation DAG."""
    dag = dagbag.get_dag("retail_transform")
    
    # Verify tasks exist
    task_ids = [task.task_id for task in dag.tasks]
    expected_tasks = [
        "dbt_debug", "dbt_deps", "dbt_run_staging", "dbt_test_staging", 
        "dbt_run_core", "dbt_test_core", "dbt_docs_generate"
    ]
    for task_id in expected_tasks:
        assert task_id in task_ids
    
    # Verify dependencies
    task_dependencies = {
        "dbt_deps": ["dbt_debug"],
        "dbt_run_staging": ["dbt_deps"],
        "dbt_test_staging": ["dbt_run_staging"],
        "dbt_run_core": ["dbt_test_staging"],
        "dbt_test_core": ["dbt_run_core"],
        "dbt_docs_generate": ["dbt_test_core"]
    }
    
    for task_id, upstream_task_ids in task_dependencies.items():
        task = dag.get_task(task_id)
        upstream_task_ids_set = {t.task_id for t in task.upstream_list}
        assert set(upstream_task_ids) == upstream_task_ids_set


def test_dbt_transform_dag_default_args(dagbag):
    """Test the default arguments of the dbt transformation DAG."""
    dag = dagbag.get_dag("retail_transform")
    
    assert dag.default_args["owner"] == "data_engineer"
    assert dag.default_args["retries"] == 3
    assert dag.schedule_interval is None  # No automatic scheduling
    assert dag.catchup is False
    assert "transform" in dag.tags
    assert "dbt" in dag.tags
    assert "retail" in dag.tags
