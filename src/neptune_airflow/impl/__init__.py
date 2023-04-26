#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__all__ = [
    "__version__",
    "get_run_from_context",
    "get_task_handler_from_context",
]

from hashlib import md5
from typing import (
    Any,
    Dict,
    Optional,
)

from airflow.models import Variable

try:
    from neptune import (
        Run,
        init_run,
    )
    from neptune.handler import Handler
    from neptune.utils import stringify_unsupported
except ImportError:
    from neptune.new import (
        init_run,
        Run,
    )
    from neptune.new.handler import Handler
    from neptune.new.utils import stringify_unsupported

from neptune_airflow.impl.version import __version__

INTEGRATION_VERSION_KEY = "source_code/integrations/airflow"

INDIVIDUAL_TASK_KEYS = {"task", "task_instance", "task_instance_key_str", "ti"}


def _log_context(context: Dict[str, Any], neptune_run: Run) -> None:
    for field in {"conf", "dag", "dag_run"}:
        to_log = context.pop(field, None)
        if to_log:
            neptune_run[f"context/{field}"] = stringify_unsupported(to_log.__dict__)
    for key in context:
        if key not in INDIVIDUAL_TASK_KEYS:
            neptune_run[f"context/{key}"] = str(context[key])


def get_run_from_context(
    *,
    api_token: Optional[str] = None,
    project: Optional[str] = None,
    log_context: bool = False,
    context: Dict[str, Any],
) -> Run:
    """Creates a Neptune run based on the relevant DAG run.

    This ensures that different tasks will still log to the same run if this function is used.
    You can log the context of the task automatically by setting the `log_context` parameter to `True`.

    Args:
        api_token: Neptune API token. If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used.
            Note: To keep your token secure, avoid placing it in the source code. Instead,
            save it as an environment variable. You can also use Airflow variables:
            https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
        project: Name of the Neptune project you want to log to, in the form "workspace-name/project-name".
            If `None`, the value of the `NEPTUNE_PROJECT` environment variable is used.
            You can also use Airflow variables:
            https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
        log_context: If True, the task context will also be logged to the run.
        context: The task context obtained from Airflow.

    Returns:
        `Run` with CUSTOM_RUN_ID based on DAG run ID.

    For examples, see the examples/ directory.

    For more, see the docs:
        Tutorial: https://docs.neptune.ai/integrations/airflow/
        API reference: https://docs.neptune.ai/api/integrations/airflow/
    """

    # Check airflow variables if api_token or project not provided
    api_token = api_token or Variable.get("NEPTUNE_API_TOKEN", None)

    project = project or Variable.get("NEPTUNE_PROJECT", None)

    dag_run_id = context["dag_run"].run_id
    run = init_run(
        api_token=api_token,
        project=project,
        custom_run_id=md5(dag_run_id.encode()).hexdigest(),  # CUSTOM_RUN_ID max length = 32
    )

    if not run.exists(INTEGRATION_VERSION_KEY):
        run[INTEGRATION_VERSION_KEY] = __version__

    if log_context and not run.exists("context"):
        _log_context(context, run)

    return run


def get_task_handler_from_context(
    *,
    api_token: Optional[str] = None,
    project: Optional[str] = None,
    log_context: bool = False,
    context: Dict[str, Any],
) -> Handler:
    """Creates a Neptune handler based on the relevant DAG run and task ID.

    This ensures that different tasks will still log to the same run if this function is used.
    The metadata from each task is logged under separate namespaces within the run.
    You can log the context of the task automatically by setting the `log_context` parameter to `True`.

    Args:
        api_token: Neptune API token. If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used.
            Note: To keep your token secure, avoid placing it in the source code. Instead,
            save it as an environment variable. You can also use Airflow variables:
            https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
        project: Name of the Neptune project you want to log to, in the form "workspace-name/project-name".
            If `None`, the value of the `NEPTUNE_PROJECT` environment variable is used.
            You can also use Airflow variables:
            https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
        log_context: If `True`, the task context will also be logged to the run.
        context: The task context obtained from Airflow.

    Returns:
        `Handler` with CUSTOM_RUN_ID based on DAG run ID and namespace based on task ID.

    For examples, see the examples/ directory.

    For more, see the docs:
        Tutorial: https://docs.neptune.ai/integrations/airflow/
        API reference: https://docs.neptune.ai/api/integrations/airflow/
    """
    base_namespace = context["ti"].task_id
    run = get_run_from_context(api_token=api_token, project=project, log_context=log_context, context=context)

    if log_context:
        for key in INDIVIDUAL_TASK_KEYS:
            if key in context:
                run[base_namespace][f"context/{key}"] = str(context[key])

    return run[base_namespace]
