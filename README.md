# Neptune + Airflow integration

Experiment tracking for Airflow DAG runs.

## Installation

```
pip install -U neptune-airflow
```

## Example

```python
from neptune_airflow import NeptuneLogger

with DAG(
    ...
) as dag:
    def task(**context):
        neptune_logger = NeptuneLogger()
```

For more instructions and examples, see the [Airflow integration guide](https://docs.neptune.ai/integrations/airflow/) in the Neptune documentation.

## Support

If you got stuck or simply want to talk to us, here are your options:

* Check our [FAQ page](https://docs.neptune.ai/getting_help).
* You can submit bug reports, feature requests, or contributions directly to the repository.
* Chat! In the Neptune app, click the blue message icon in the bottom-right corner and send a message. A real person will talk to you ASAP (typically very ASAP).
* You can just shoot us an email at [support@neptune.ai](mailto:support@neptune.ai).
