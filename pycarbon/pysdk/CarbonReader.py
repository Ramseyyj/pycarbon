import ctypes

import pyarrow as pa
from modelarts import manifest
from modelarts.field_name import CARBON

from pycarbon.Constants import LOCAL_FILE_PREFIX


class CarbonReader(object):
    def __init__(self):
        from jnius import autoclass
        self.readerClass = autoclass('org.apache.carbondata.sdk.file.ArrowCarbonReader')

    def builder(self, input_split):
      self.input_split = input_split
      self.CarbonReaderBuilder = self.readerClass.builder(input_split)
      return self

    def projection(self, projection_list):
        self.CarbonReaderBuilder.projection(projection_list)
        return self

    def withHadoopConf(self, key, value):
      if "fs.s3a.access.key" == key:
        self.ak = value
      elif "fs.s3a.secret.key" == key:
        self.sk = value
      elif "fs.s3a.endpoint" == key:
        self.end_point = value
      elif "fs.s3a.proxy.host" == key:
        self.host = value
      elif "fs.s3a.proxy.port" == key:
        self.port = value

      self.CarbonReaderBuilder.withHadoopConf(key, value)
      return self

    def build(self):
        self.reader = self.CarbonReaderBuilder.buildArrowReader()
        return self

    def withFileLists(self, list):
      self.CarbonReaderBuilder.withFileLists(list)
      return self

    def getSplits(self, is_blocklet_split):
      from jnius import autoclass

      java_list_class = autoclass('java.util.ArrayList')

      if str(self.input_split).endswith(".manifest"):
        if str(self.input_split).startswith(LOCAL_FILE_PREFIX):
          self.manifest_path = str(self.input_split)[len(LOCAL_FILE_PREFIX):]
        else:
          self.manifest_path = self.input_split

        from obs import ObsClient
        if str(self.input_split).startswith("s3"):
          obsClient = ObsClient(access_key_id=self.ak, secret_access_key=self.sk,
                                server=str(self.end_point).replace('http://', ''),
                                long_conn_mode=True)
          sources = manifest.getSources(self.manifest_path, CARBON, obsClient)
          self.file_path = sources[0]
        else:
          sources = manifest.getSources(self.manifest_path, CARBON)
        java_list = java_list_class()
        for source in sources:
          java_list.add(source)
        return self.CarbonReaderBuilder.withFileLists(java_list).getSplits(is_blocklet_split)
      else:
        return self.CarbonReaderBuilder.getSplits(is_blocklet_split)

    def read(self, schema):
        address = self.reader.readArrowBatchAddress(schema)
        size = (ctypes.c_int32).from_address(address).value
        arrowData = (ctypes.c_byte * size).from_address(address + 4)
        rawData = bytes(arrowData)
        self.reader.freeArrowBatchMemory(address)
        reader = pa.RecordBatchFileReader(pa.BufferReader(rawData))
        data = reader.read_all()
        return data

    def close(self):
        return self.reader.close()
