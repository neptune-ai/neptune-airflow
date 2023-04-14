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
]

from typing import (
    Any,
    Dict,
    Optional,
)

from airflow.models import Variable
from neptune import (
    Run,
    init_run,
)

from neptune_airflow.impl.version import __version__

INTEGRATION_VERSION_KEY = "source_code/integrations/airflow"


def get_run_from_context(
    *,
    api_token: Optional[str],
    project: Optional[str],
    log_context: bool = False,
    context: Dict[str, Any],
) -> Run:

    # Check airflow variables if api_token or project not provided
    if not api_token:
        api_token = Variable.get("NEPTUNE_API_TOKEN", None)

    if not project:
        project = Variable.get("NEPTUNE_PROJECT", None)

    dag_run_id = context["dag_run"].run_id
    run = init_run(
        api_token=api_token,
        project=project,
        with_id=dag_run_id,
    )

    if not run.exists(INTEGRATION_VERSION_KEY):
        run[INTEGRATION_VERSION_KEY] = __version__

    if log_context:
        # log context using stringify_unsupported
        ...

    return run
