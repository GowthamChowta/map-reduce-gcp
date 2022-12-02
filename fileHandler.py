from storageHandler import GoogleFireStore
from utils.util import read_ini

# The `project` parameter is optional and represents which project the client
# will act on behalf of. If not supplied, the client falls back to the default
# project inferred from the environment.


params = read_ini()

noOfMappers = int(params["APPLICATION"]["NOOFMAPPERS"])

class FileHandler:
    def __init__(self, path, noOfMappers):
        self.path = path
        self.startIndex = 0
        self.chunkSize = -1
        self.noOfMappers = noOfMappers
        self.data = None
        self.loadFile()
        self.updateChunkSize()        

    def loadFile(self):
        with open(self.path) as f:
            self.data = f.readlines()
            self.numberOfLines = len(self.data)

    def updateChunkSize(self):

        self.chunkSize = self.numberOfLines // self.noOfMappers

    def readNextChunk(self):

        self.chunk = " ".join(self.data[self.startIndex : self.startIndex + self.chunkSize])
        self.startIndex += self.chunkSize

    def saveChunkSizeToFireStore(self,c):
        c.save(["chunkSize",self.chunkSize])
        c.save(["status","yettostart"])
        c.save(["curIndex",self.numberOfLines])

    def sendNextChunkToKeyValueServer(self, key,c):
        self.readNextChunk()
        print("Key is",key," and next chunk is",self.chunk[:100])
        c.save([key, self.chunk])
        
        
        
# Comment this line when done
g = GoogleFireStore()
f = FileHandler("data/book_small.txt",noOfMappers)
for i in range(noOfMappers):
    f.sendNextChunkToKeyValueServer("Mapper-"+str(i),g)
f.saveChunkSizeToFireStore(g)