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

__all__ = ["__version__", "NeptuneLogger"]

import warnings
from contextlib import contextmanager
from copy import copy
from hashlib import md5
from typing import (
    Any,
    Dict,
    Optional,
    Union,
)

from airflow.models import Variable
from neptune.internal.state import ContainerState

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


def singleton(class_):
    # https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python

    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


@singleton
class NeptuneLogger:
    """Creates a Neptune logger instance for tracking metadata during a DAG run.

    Args:
        api_token: User's Neptune API token.
            If None, the value of the `NEPTUNE_API_TOKEN` Airflow Variable is used (recommended).
            For help, see https://docs.neptune.ai/setup/setting_api_token/
        project: Name of the Neptune project where the metadata should go.
            If None, the value of the `NEPTUNE_PROJECT` Airflow Variable is used (recommended).
            The full name of the project has the form `workspace-name/project-name`.
            For help, see https://docs.neptune.ai/setup/setting_project_name/
        **neptune_kwargs: Additional keyword arguments to be passed directly to the `init_run()` function, such as
            `description` and `tags`. For more, see https://docs.neptune.ai/api/neptune/#init_run
            Note: The `custom_run_id` parameter is reserved. It's automatically generated based on the DAG ID.

    Example:
        from neptune_airflow import NeptuneLogger

        with DAG(
            ...
        ) as dag:
            def task_1(**context):
                neptune_logger = NeptuneLogger()

    For more, see the docs:
        Tutorial: https://docs.neptune.ai/integrations/airflow/
        API reference: https://docs.neptune.ai/api/integrations/airflow/
    """

    def __init__(
        self,
        *,
        api_token: Optional[str] = None,
        project: Optional[str] = None,
        **neptune_kwargs,
    ) -> None:
        self.api_token = api_token or Variable.get("NEPTUNE_API_TOKEN", None)
        self.project = project or Variable.get("NEPTUNE_PROJECT", None)
        self.neptune_kwargs = neptune_kwargs
        if "custom_run_id" in self.neptune_kwargs:
            ci = self.neptune_kwargs.pop("custom_run_id")
            warnings.warn(f"Given custom_run_id ('{ci}') will be overwritten")

        self.run = None
        self.base_handler = None
        self.dag_run_id = None

    def _initialize_run(self, context: Dict[str, Any], log_context: bool = False) -> Run:
        dag_run_id = context["dag_run"].run_id
        run = init_run(
            api_token=self.api_token,
            project=self.project,
            custom_run_id=md5(dag_run_id.encode()).hexdigest(),  # CUSTOM_RUN_ID max length = 32
            **self.neptune_kwargs,
        )
        if not run.exists(INTEGRATION_VERSION_KEY):
            run[INTEGRATION_VERSION_KEY] = __version__

        if log_context and not run.exists("context"):
            _log_context(context, run)

        return run

    @contextmanager
    def get_run_from_context(self, context: Dict[str, Any], log_context: bool = False) -> Run:
        if self.run and self.run._state == ContainerState.STOPPED:
            self.run = None

        if not self.run or self.dag_run_id != context["dag_run"].run_id:
            self.dag_run_id = context["dag_run"].run_id
            self.run = self._initialize_run(context, log_context)

        yield self.run

        self.run.sync()

    @contextmanager
    def get_task_handler_from_context(self, context: Dict[str, Any], log_context: bool = False) -> Handler:
        if not self.base_handler or self.dag_run_id != context["dag_run"].run_id:
            base_namespace = context["ti"].task_id
            with self.get_run_from_context(context, False) as run:
                self.base_handler = run[base_namespace]
                if log_context:
                    _log_context(context, self.base_handler)

                yield self.base_handler

                self.base_handler = None


def _log_context(context: Dict[str, Any], neptune_run: Union[Run, Handler]) -> None:
    _context = copy(context)
    for field in {"conf", "dag", "dag_run"}:
        to_log = _context.pop(field, None)
        if to_log:
            if field == "conf":
                inversed_deprecated_options_dict = to_log.__dict__.pop("inversed_deprecated_options")
                for key, value in inversed_deprecated_options_dict.items():
                    if isinstance(key, tuple):
                        key = "_".join(key)
                    if isinstance(value, tuple):
                        value = "_".join(value)

                    neptune_run[f"context/{field}/inversed_deprecated_options/{key}"] = stringify_unsupported(value)

            neptune_run[f"context/{field}"] = stringify_unsupported(to_log.__dict__)

    for key in _context:
        neptune_run[f"context/{key}"] = str(_context[key])
