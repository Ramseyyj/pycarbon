#  Copyright (c) 2017-2018 Uber Technologies, Inc.
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

import mock
import unittest

from pyarrow.filesystem import LocalFileSystem, S3FSWrapper
from pyarrow.lib import ArrowIOError
from six.moves.urllib.parse import urlparse

from petastorm.hdfs.tests.test_hdfs_namenode import HC, MockHadoopConfiguration, \
  MockHdfs, MockHdfsConnector

from pycarbon.carbon_fs_utils import CarbonFilesystemResolver

ABS_PATH = '/abs/path'


class FilesystemResolverTest(unittest.TestCase):
  """
  Checks the full filesystem resolution functionality, exercising each URL interpretation case.
  """

  @classmethod
  def setUpClass(cls):
    cls.mock = MockHdfsConnector()

  def setUp(self):
    """Initializes a mock hadoop config and populate with basic properties."""
    # Reset counters in mock connector
    self.mock.reset()
    self._hadoop_configuration = MockHadoopConfiguration()
    self._hadoop_configuration.set('fs.defaultFS', HC.FS_WARP_TURTLE)
    self._hadoop_configuration.set('dfs.ha.namenodes.{}'.format(HC.WARP_TURTLE), 'nn2,nn1')
    self._hadoop_configuration.set('dfs.namenode.rpc-address.{}.nn1'.format(HC.WARP_TURTLE), HC.WARP_TURTLE_NN1)
    self._hadoop_configuration.set('dfs.namenode.rpc-address.{}.nn2'.format(HC.WARP_TURTLE), HC.WARP_TURTLE_NN2)

  def test_error_url_cases(self):
    """Various error cases that result in exception raised."""
    # Case 1: Schemeless path asserts
    with self.assertRaises(ValueError):
      CarbonFilesystemResolver(ABS_PATH, {})

    # Case 4b: HDFS default path case with NO defaultFS
    with self.assertRaises(RuntimeError):
      CarbonFilesystemResolver('hdfs:///some/path', {})

    # Case 4b: Using `default` as host, while apparently a pyarrow convention, is NOT valid
    with self.assertRaises(ArrowIOError):
      CarbonFilesystemResolver('hdfs://default', {})

    # Case 5: other schemes result in ValueError; urlparse to cover an else branch!
    with self.assertRaises(ValueError):
      CarbonFilesystemResolver(urlparse('http://foo/bar'), {})
    with self.assertRaises(ValueError):
      CarbonFilesystemResolver(urlparse('ftp://foo/bar'), {})
    with self.assertRaises(ValueError):
      CarbonFilesystemResolver(urlparse('ssh://foo/bar'), {})

    # s3 paths must have the bucket as the netloc
    with self.assertRaises(ValueError):
      CarbonFilesystemResolver(urlparse('s3:///foo/bar'), {})

  def test_file_url(self):
    """ Case 2: File path, agnostic to content of hadoop configuration."""
    suj = CarbonFilesystemResolver('file://{}'.format(ABS_PATH), hadoop_configuration=self._hadoop_configuration,
                                   connector=self.mock)
    self.assertTrue(isinstance(suj.filesystem(), LocalFileSystem))
    self.assertEqual('', suj.parsed_dataset_url().netloc)
    self.assertEqual(ABS_PATH, suj.get_dataset_path())

  def test_hdfs_url_with_nameservice(self):
    """ Case 3a: HDFS nameservice."""
    suj = CarbonFilesystemResolver(dataset_url=HC.WARP_TURTLE_PATH, hadoop_configuration=self._hadoop_configuration,
                                   connector=self.mock)
    self.assertEqual(MockHdfs, type(suj.filesystem()._hdfs))
    self.assertEqual(HC.WARP_TURTLE, suj.parsed_dataset_url().netloc)
    self.assertEqual(1, self.mock.connect_attempted(HC.WARP_TURTLE_NN2))
    self.assertEqual(0, self.mock.connect_attempted(HC.WARP_TURTLE_NN1))
    self.assertEqual(0, self.mock.connect_attempted(HC.DEFAULT_NN))

  def test_hdfs_url_no_nameservice(self):
    """ Case 3b: HDFS with no nameservice should connect to default namenode."""
    suj = CarbonFilesystemResolver(dataset_url='hdfs:///some/path', hadoop_configuration=self._hadoop_configuration,
                                   connector=self.mock)
    self.assertEqual(MockHdfs, type(suj.filesystem()._hdfs))
    self.assertEqual(HC.WARP_TURTLE, suj.parsed_dataset_url().netloc)
    # ensure path is preserved in parsed URL
    self.assertEqual('/some/path', suj.get_dataset_path())
    self.assertEqual(1, self.mock.connect_attempted(HC.WARP_TURTLE_NN2))
    self.assertEqual(0, self.mock.connect_attempted(HC.WARP_TURTLE_NN1))
    self.assertEqual(0, self.mock.connect_attempted(HC.DEFAULT_NN))

  def test_hdfs_url_direct_namenode(self):
    """ Case 4: direct namenode."""
    suj = CarbonFilesystemResolver('hdfs://{}/path'.format(HC.WARP_TURTLE_NN1),
                                   hadoop_configuration=self._hadoop_configuration,
                                   connector=self.mock)
    self.assertEqual(MockHdfs, type(suj.filesystem()))
    self.assertEqual(HC.WARP_TURTLE_NN1, suj.parsed_dataset_url().netloc)
    self.assertEqual(0, self.mock.connect_attempted(HC.WARP_TURTLE_NN2))
    self.assertEqual(1, self.mock.connect_attempted(HC.WARP_TURTLE_NN1))
    self.assertEqual(0, self.mock.connect_attempted(HC.DEFAULT_NN))

  def test_hdfs_url_direct_namenode_retries(self):
    """ Case 4: direct namenode fails first two times thru, but 2nd retry succeeds."""
    self.mock.set_fail_n_next_connect(2)
    with self.assertRaises(ArrowIOError):
      suj = CarbonFilesystemResolver('hdfs://{}/path'.format(HC.WARP_TURTLE_NN2),
                                     hadoop_configuration=self._hadoop_configuration,
                                     connector=self.mock)
    self.assertEqual(1, self.mock.connect_attempted(HC.WARP_TURTLE_NN2))
    self.assertEqual(0, self.mock.connect_attempted(HC.WARP_TURTLE_NN1))
    self.assertEqual(0, self.mock.connect_attempted(HC.DEFAULT_NN))
    with self.assertRaises(ArrowIOError):
      suj = CarbonFilesystemResolver('hdfs://{}/path'.format(HC.WARP_TURTLE_NN2),
                                     hadoop_configuration=self._hadoop_configuration,
                                     connector=self.mock)
    self.assertEqual(2, self.mock.connect_attempted(HC.WARP_TURTLE_NN2))
    self.assertEqual(0, self.mock.connect_attempted(HC.WARP_TURTLE_NN1))
    self.assertEqual(0, self.mock.connect_attempted(HC.DEFAULT_NN))
    # this one should connect "successfully"
    suj = CarbonFilesystemResolver('hdfs://{}/path'.format(HC.WARP_TURTLE_NN2),
                                   hadoop_configuration=self._hadoop_configuration,
                                   connector=self.mock)
    self.assertEqual(MockHdfs, type(suj.filesystem()))
    self.assertEqual(HC.WARP_TURTLE_NN2, suj.parsed_dataset_url().netloc)
    self.assertEqual(3, self.mock.connect_attempted(HC.WARP_TURTLE_NN2))
    self.assertEqual(0, self.mock.connect_attempted(HC.WARP_TURTLE_NN1))
    self.assertEqual(0, self.mock.connect_attempted(HC.DEFAULT_NN))

  def test_s3_without_s3fs(self):
    with mock.patch.dict('sys.modules', s3fs=None):
      # `import s3fs` will fail in this context
      with self.assertRaises(ValueError):
        CarbonFilesystemResolver(urlparse('s3a://foo/bar'), {})

  def test_s3_url(self):
    suj = CarbonFilesystemResolver('s3a://bucket{}'.format(ABS_PATH),
                                   key='OF0FTHGASIHDTRYHBCWU',
                                   secret='fWWjJwh89NFaMDPrFdhu68Umus4vftlIzcNuXvwV',
                                   endpoint="http://obs.cn-north-5.myhuaweicloud.com",
                                   hadoop_configuration=self._hadoop_configuration, connector=self.mock)
    self.assertTrue(isinstance(suj.filesystem(), S3FSWrapper))
    self.assertEqual('bucket', suj.parsed_dataset_url().netloc)
    self.assertEqual('bucket' + ABS_PATH, suj.get_dataset_path())


if __name__ == '__main__':
  unittest.main()
