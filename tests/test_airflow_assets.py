from pathlib import Path


def test_airflow_dag_file_exists():
    dag_file = Path("dags/brics_currency_pipeline_dag.py")

    assert dag_file.exists()
    contents = dag_file.read_text(encoding="utf-8")
    assert 'dag_id="brics_currency_pipeline"' in contents
    assert "extract_task" in contents
    assert "transform_task" in contents
    assert "gold_task" in contents
    assert "load_task" in contents
    assert "build_and_save_gold_data" in contents
    assert "apply_migrations()" in contents
    assert 'pd.read_parquet(gold_result["processed_file"])' in contents
