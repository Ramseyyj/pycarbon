#  Copyright (c) 2018-2019 Huawei Technologies, Inc.
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
import os

from pycarbon.pysdk.CarbonReader import CarbonReader
import time

from pycarbon.pysdk.CarbonWriter import CarbonWriter
from pycarbon.pysdk.SDKUtil import SDKUtil
from pycarbon.tests import DEFAULT_CARBONSDK_PATH

import unittest


class WriteCarbonTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    import jnius_config

    jnius_config.set_classpath(DEFAULT_CARBONSDK_PATH)

    print("TestCase  start running ")

  def test_1_run_write_carbon(self):

    start = time.time()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int}]";
    path = "/tmp/data/writeCarbon" + str(time.time())
    print(path)
    writer = CarbonWriter() \
      .builder() \
      .outputPath(path) \
      .withCsvInput(jsonSchema) \
      .writtenBy("pycarbon") \
      .build()

    for i in range(0, 10):
      from jnius import autoclass
      arrayListClass = autoclass("java.util.ArrayList");
      list = arrayListClass()
      list.add("pycarbon")
      list.add(str(i))
      list.add(str(i * 10))
      writer.write(list.toArray())
    writer.close()

    reader = CarbonReader() \
      .builder() \
      .withFolder(path) \
      .withBatch(1000) \
      .build()

    i = 0
    while (reader.hasNext()):
      rows = reader.readNextBatchRow()
      for row in rows:
        i = i + 1
        print()
        if 1 == i % 10:
          print(str(i) + " to " + str(i + 9) + ":")
        for column in row:
          print(column, end="\t")

    print()
    print("number of rows by read: " + str(i))
    assert 10 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")

  def test_2_run_write_carbon_binary(self):

    start = time.time()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int},{binaryField:binary}]";
    path = "/tmp/data/writeCarbon" + str(time.time())
    out_path = "/tmp/data/writeCarbon2_" + str(time.time()) + "/image.jpg"
    jpg_path = "../../data/image/carbondatalogo.jpg"
    print(path)
    writer = CarbonWriter() \
      .builder() \
      .outputPath(path) \
      .withCsvInput(jsonSchema) \
      .writtenBy("pycarbon") \
      .build()

    with open(jpg_path, mode='rb+') as file_object:
      content = file_object.read()

    bytesdata = bytes(content)
    sdkUtil = SDKUtil()
    for i in range(0, 10):
      from jnius import autoclass
      arrayListClass = autoclass("java.util.ArrayList");
      list = arrayListClass()
      list.add("pycarbon")
      list.add(str(i))
      list.add(str(i * 10))
      jppg_bytes = sdkUtil.SDKUtilClass.readBinary(jpg_path)
      list.add(bytearray(jppg_bytes))
      list.add(jppg_bytes)
      list.add(jppg_bytes)
      writer.write(list.toArray())
    writer.close()

    reader = CarbonReader() \
      .builder() \
      .withFolder(path) \
      .withBatch(1000) \
      .build()

    i = 0
    while (reader.hasNext()):
      rows = reader.readNextBatchRow()
      for row in rows:
        i = i + 1
        print()
        if 1 == i % 10:
          print(str(i) + " to " + str(i + 9) + ":")
        for column in row:
          print(column, end="\t")
          from jnius.jnius import ByteArray
          if 1 == i and isinstance(column, ByteArray) and len(column) > 1000:
            with open(path + "/image.jpg", 'w+', encoding="ISO-8859-1") as file_object:
              file_object.write(str(column.tostring(), 'utf-8'))

    print()
    print("number of rows by read: " + str(i))
    assert 10 == i
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")

  def test_3_run_write_image(self):
    path = "/Users/xubo/Desktop/xubo/git/pycarbon/pycarbon/tests/data/image/carbondatalogo.jpg"
    a = 2 + 1
    print(a)
    from skimage import io
    img = io.imread(path)
    io.imshow(img)

    with open(path, mode='rb+') as file_object:
      content2 = file_object.read()
      bytesdata = bytes(content2)
      print(len(content2))
      print(len(bytesdata))

    with open(path, encoding="ISO-8859-1") as file_object:
      content2 = file_object.read()
      bytesdata = bytes(content2, encoding="ISO-8859-1")
      print(len(content2))
      print(len(bytesdata))
