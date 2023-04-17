import os
from datetime import (
    datetime,
    timedelta,
)
from hashlib import md5

import neptune
from airflow import DAG
from airflow.decorators import task

from neptune_airflow import get_run_from_context


def test_e2e():
    with DAG(
        dag_id="test_dag",
        description="test_description",
        schedule="@daily",
        start_date=datetime.today() - timedelta(days=1),
    ) as dag:

        @task(task_id="hello")
        def task1(**context):
            neptune_run = get_run_from_context(context=context)
            neptune_run["some_metric"] = 5
            os.environ["NEPTUNE_CUSTOM_RUN_ID"] = md5(context["dag_run"].run_id.encode()).hexdigest()
            neptune_run.sync()
            neptune_run.stop()

        task1()

        dag.test()

    run = neptune.init_run()
    assert run["some_metric"].fetch() == 5
