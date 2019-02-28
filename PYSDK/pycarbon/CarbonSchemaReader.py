class CarbonSchemaReader(object):
    def __init__(self):
        from jnius import autoclass
        self.carbonSchemaReader = autoclass('org.apache.carbondata.sdk.file.CarbonSchemaReader')

    def readSchema(self, path, getAsBuffer=False, *para):
        if (getAsBuffer == True):
            return self.carbonSchemaReader.getSchemaAsBytes(path)
        if (len(para) == 0):
            return self.carbonSchemaReader.readSchema(path).asOriginOrder()
        if (len(para) == 1):
            return self.carbonSchemaReader.readSchema(path, para[0]).asOriginOrder()
        if (len(para) == 2):
            return self.carbonSchemaReader.readSchema(path, para[0], para[1]).asOriginOrder()