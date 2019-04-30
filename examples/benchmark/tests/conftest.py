import pytest


def pytest_addoption(parser):
  parser.addoption('--carbon-sdk-path', type=str, default='../../../jars/carbondata-sdk.jar',
                   help='carbon sdk path')
