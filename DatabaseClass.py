import random
import tempfile

class DatabaseClass:

    instanceCount = 0

    instanceSeedID=99

    tempfile.tempdir = tempfile.mkdtemp()
    
    def __init__(self, description: str):
        DatabaseClass.instanceCount += 1
        DatabaseClass.instanceSeedID = random.randint(DatabaseClass.instanceSeedID+1,
                                                    DatabaseClass.instanceSeedID+100)
        self.description = description

    def createTempFile(self):
        try:
            self.tempFile = tempfile.NamedTemporaryFile()
        except:
            print("Exception in tempFile creation")

    def closeTempFile(self):
        try:
            self.tempFile.close()
        except:
            print("Exception closing tempFile")

    def getTempFileName(self):
        return self.tempFile.name

    def writeTempFileName(self, inputBytes):
        self.tempFile.write(inputBytes)

    def readTempFileName(self):
        self.tempFile.seek(0)
        return self.tempFile.read()

    def getDataBaseName(self):
        return self.dataBaseName

    def setDataBaseName(self, dataBaseName: str):
        self.dataBaseName = dataBaseName

    def getDescription(self):
        return self.description
    
    def setDescription(self, description: str):
        self.description = description

    def getInstanceCount(self):
        return DatabaseClass.instanceCount
    
    def __del__(self, description: str = "None", instanceID: int = None):
        print("Deleted - "+description+" InstanceID="+str(instanceID))
        DatabaseClass.instanceCount -= 1

    def printClassFunctionName(className: str, functionName: str):
        print("==============================>" + className + "." + functionName)