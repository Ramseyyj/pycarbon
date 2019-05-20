from examples.benchmark.external_dataset import carbon_batch_read_from_local
from examples.benchmark.external_dataset import carbon_batch_read_with_projection
from examples.benchmark.external_dataset import carbon_batch_read_with_shuffle
from examples.benchmark.external_dataset import generate_benchmark_external_dataset

import os
import pytest
import jnius_config

jnius_config.set_classpath(pytest.config.getoption("--carbon-sdk-path"))

jnius_config.add_options('-Xrs', '-Xmx6g')

if pytest.config.getoption("--pyspark-python") is not None and \
    pytest.config.getoption("--pyspark-driver-python") is not None:
  os.environ['PYSPARK_PYTHON'] = pytest.config.getoption("--pyspark-python")
  os.environ['PYSPARK_DRIVER_PYTHON'] = pytest.config.getoption("--pyspark-driver-python")
elif 'PYSPARK_PYTHON' in os.environ.keys() and 'PYSPARK_DRIVER_PYTHON' in os.environ.keys():
  pass
else:
  raise ValueError("please set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON variables, "
                   "using cmd line --pyspark-python=PYSPARK_PYTHON_PATH --pyspark-driver-python=PYSPARK_DRIVER_PYTHON_PATH, "
                   "or set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON in system env")


generate_benchmark_external_dataset.generate_benchmark_dataset()


def test_carbon_batch_read():
    num_1 = carbon_batch_read_from_local.just_read_batch()
    num_2 = carbon_batch_read_from_local.just_read_batch()

    assert num_1 == num_2


def test_carbon_batch_read_with_projection():
    num_1 = carbon_batch_read_with_projection.just_read_batch()
    num_2 = carbon_batch_read_with_projection.just_read_batch()

    assert num_1 == num_2


def test_carbon_batch_read_with_shuffle():
    list_1 = carbon_batch_read_with_shuffle.just_read_batch()
    list_2 = carbon_batch_read_with_shuffle.just_read_batch()

    assert list_1 != list_2
