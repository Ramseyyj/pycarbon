import pyarrow as pa
from pyarrow.filesystem import (_get_fs_from_path)
from pyarrow.parquet import ParquetFile
from pycarbon.CarbonReader import CarbonReader
from pycarbon.CarbonSchemaReader import CarbonSchemaReader


class CarbonDataset(object):
    def __init__(self, path):
        self.path = path
        self.fs = _get_fs_from_path(path)
        self.pieces = list()
        carbon_splits = CarbonReader().builder().withFolder(self.path).getSplits()
        carbon_schema = CarbonSchemaReader().readSchema(self.path)
        for split in carbon_splits:
            self.pieces.append(CarbonDatasetPiece(split, carbon_schema))
        self.number_of_splits = len(self.pieces)
        self.schema = self.getArrowSchema()
        # TODO add mechanism to get the file path based on file filter
        self.common_metadata_path = path + '/_common_metadata'
        self.common_metadata = None
        try:
            if self.fs.exists(self.common_metadata_path):
                with self.fs.open(self.common_metadata_path) as f:
                    self.common_metadata = ParquetFile(f).metadata
        except:
            self.common_metadata = None

    def getArrowSchema(self):
        buf = CarbonSchemaReader().readSchema(self.path, True)
        reader = pa.RecordBatchFileReader(pa.BufferReader(bytes(buf)))
        return reader.read_all().schema


class CarbonDatasetPiece(object):
    def __init__(self, path, schema):
        self.path = path
        self.schema = schema
        # TODO get record count from carbonapp based on file
        self.num_rows = 10000

    def read_all(self, columns):
        # rebuilding the reader as need to read specific columns
        carbon_reader = CarbonReader().builder().withFile(self.path).projection(columns).build()
        data = carbon_reader.read(self.schema)
        carbon_reader.close()
        return data

