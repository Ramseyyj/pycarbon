import os
import time
from examples.mnist import tf_example_carbon as tf_example
from examples.mnist.generate_pycarbon_mnist import mnist_data_to_pycarbon_dataset

import pytest
import jnius_config

jnius_config.set_classpath(pytest.config.getoption("--carbon-sdk-path"))

os.environ['PYSPARK_PYTHON'] = pytest.config.getoption("--pyspark-python")
os.environ['PYSPARK_DRIVER_PYTHON'] = pytest.config.getoption("--pyspark-driver-python")


def test_full_tf_example(large_mock_mnist_data, tmpdir):
  # First, generate mock dataset
  dataset_url = 'file://{}'.format(tmpdir)
  mnist_data_to_pycarbon_dataset(tmpdir, dataset_url, mnist_data=large_mock_mnist_data,
                                 spark_master='local[1]', carbon_files_count=1)

  start = time.time()
  # Tensorflow train and test
  tf_example.train_and_test(
    dataset_url=dataset_url,
    training_iterations=10,
    batch_size=10,
    evaluation_interval=10,
    start=start
  )
