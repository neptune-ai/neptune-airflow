import os
from datetime import (
    datetime,
    timedelta,
)

import neptune
import pytest
from airflow import DAG
from airflow.decorators import task


class TestE2E:
    @pytest.mark.parametrize("log_context", [True, False])
    def test_metrics_and_context_logged_run(self, log_context, logger):
        with DAG(
            dag_id="test_dag",
            description="test_description",
            schedule="@daily",
            start_date=datetime.today() - timedelta(days=1),
        ) as dag:

            @task(task_id="hello-run")
            def task1(**context):
                neptune_run = logger.get_run_from_context(context=context, log_context=log_context)
                neptune_run["some_metric"] = 5
                os.environ["NEPTUNE_CUSTOM_RUN_ID"] = neptune_run["sys/custom_run_id"].fetch()
                neptune_run.sync()

            task1()

            dag.test()

        run = neptune.init_run()
        assert run["some_metric"].fetch() == 5
        assert run.exists("source_code/integrations/airflow")

        if log_context:
            assert run.exists("context")
            assert run.exists("context/dag")
            assert run["context/dag/_dag_id"].fetch() == "test_dag"
            assert run["context/dag/_description"].fetch() == "test_description"

    @pytest.mark.parametrize("log_context", [True, False])
    def test_metrics_and_context_logged_handler(self, log_context, logger):
        with DAG(
            dag_id="test_dag",
            description="test_description",
            schedule="@daily",
            start_date=datetime.today() - timedelta(days=1),
        ) as dag:

            @task(task_id="hello-handler")
            def task1(**context):
                neptune_handler = logger.get_task_handler_from_context(context=context, log_context=log_context)
                neptune_handler["some_metric"] = 5
                os.environ["NEPTUNE_CUSTOM_RUN_ID"] = neptune_handler.get_root_object()["sys/custom_run_id"].fetch()
                neptune_handler.get_root_object().sync()

            task1()

            dag.test()

        run = neptune.init_run()
        assert run["hello-handler/some_metric"].fetch() == 5
        assert run.exists("source_code/integrations/airflow")

        if log_context:
            assert not run.exists("context")

            assert run.exists("hello-handler/context/dag")
            assert run["hello-handler/context/dag/_dag_id"].fetch() == "test_dag"
            assert run["hello-handler/context/dag/_description"].fetch() == "test_description"

            assert run.exists("hello-handler/context/ti")
            assert run["hello-handler/context/task_instance_key_str"].fetch().startswith("test_dag__hello-handler")
