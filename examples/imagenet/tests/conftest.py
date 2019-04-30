import pytest


def pytest_addoption(parser):
  parser.addoption('--pyspark-python', type=str, required=True,
                   help='pyspark python env variable')
  parser.addoption('--pyspark-driver-python', type=str, required=True,
                   help='pyspark driver python env variable')
  parser.addoption('--carbon-sdk-path', type=str, default='../../../jars/carbondata-sdk.jar',
                   help='carbon sdk path')
