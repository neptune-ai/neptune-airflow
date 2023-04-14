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
    # TODO: add importable public names here, `neptune-client` uses `import *`
    # https://docs.python.org/3/tutorial/modules.html#importing-from-a-package
]

# TODO: use `warnings.warn` for user caused problems: https://stackoverflow.com/a/14762106/1565454


from neptune_airflow.impl.version import __version__

INTEGRATION_VERSION_KEY = "source_code/integrations/airflow"

# TODO: Implementation of neptune-airflow here
