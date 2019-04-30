from examples.benchmark import carbon_read_from_local

import pytest
import os
import jnius_config

jnius_config.set_classpath(pytest.config.getoption("--carbon-sdk-path"))

jnius_config.add_options('-Xrs', '-Xmx6g')

datapath = os.path.abspath(os.path.join(os.getcwd(), ".."))
dataset_url = "file://" + datapath + "/data/tinyvoc"


def test_carbon_read():
  for i in range(2):
    print("\n\n\n number of " + str(i))
    carbon_read_from_local.just_read(dataset_url)
    carbon_read_from_local.just_read_batch(dataset_url)
