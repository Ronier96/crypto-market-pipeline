from airflow.models import DagBag


DAG_ID = "crypto_market_pipeline"

EXPECTED_TASKS = {
    "start_pipeline_run",
    "extract_and_load_landing",
    "quality_check",
    "bronze_upsert",
    "silver_upsert",
    "gold_upsert",
}


def _get_dag():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    return dagbag, dagbag.get_dag(DAG_ID)


def test_dag_imports_without_errors():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert dagbag.import_errors == {}


def test_dag_exists():
    _, dag = _get_dag()
    assert dag is not None


def test_dag_has_expected_tasks():
    _, dag = _get_dag()
    assert set(dag.task_ids) == EXPECTED_TASKS


def test_dag_task_dependencies():
    _, dag = _get_dag()

    assert dag.get_task("start_pipeline_run").downstream_task_ids == {
        "extract_and_load_landing"
    }

    assert dag.get_task("extract_and_load_landing").downstream_task_ids == {
        "quality_check"
    }

    assert dag.get_task("quality_check").downstream_task_ids == {
        "bronze_upsert"
    }

    assert dag.get_task("bronze_upsert").downstream_task_ids == {
        "silver_upsert"
    }

    assert dag.get_task("silver_upsert").downstream_task_ids == {
        "gold_upsert"
    }

    assert dag.get_task("gold_upsert").downstream_task_ids == set()