import pytest

from neptune_airflow import NeptuneLogger


@pytest.fixture(scope="session")
def logger() -> NeptuneLogger:
    return NeptuneLogger()
