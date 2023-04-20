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


def get_run_from_context(
    *,
    api_token: Optional[str] = None,
    project: Optional[str] = None,
    log_context: bool = False,
    context: Dict[str, Any],
) -> Run:

    # Check airflow variables if api_token or project not provided

    api_token = api_token or Variable.get("NEPTUNE_API_TOKEN", None)

    project = project or Variable.get("NEPTUNE_PROJECT", None)

    dag_run_id = context["dag_run"].run_id
    run = init_run(
        api_token=api_token,
        project=project,
        custom_run_id=md5(dag_run_id.encode()).hexdigest(),
    )

    if not run.exists(INTEGRATION_VERSION_KEY):
        run[INTEGRATION_VERSION_KEY] = __version__

    if log_context:
        conf = context.pop("conf", None)
        if conf:
            run["context/conf"] = stringify_unsupported(conf.__dict__)
        dag = context.pop("dag", None)
        if dag:
            run["context/dag"] = stringify_unsupported(dag.__dict__)

    return run


def get_task_handler_from_context(
    *,
    api_token: Optional[str] = None,
    project: Optional[str] = None,
    log_context: bool = False,
    context: Dict[str, Any],
) -> Handler:

    base_namespace = context["ti"].task_id
    run = get_run_from_context(api_token=api_token, project=project, log_context=log_context, context=context)
    return run[base_namespace]
