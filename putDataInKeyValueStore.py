from abc import ABC, abstractmethod
import json
import os
from google.cloud import firestore, storage

from constants import NEWLINESEPERATOR, NOTEXISTS
from gcpPythonClientHelper import read_ini

# The `project` parameter is optional and represents which project the client
# will act on behalf of. If not supplied, the client falls back to the default
# project inferred from the environment.


params = read_ini()

projectId = params["USER"]["PROJECTID"]
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

    def sendNextChunkToKeyValueServer(self, key,c):
        self.readNextChunk()
        print("Key is",key," and next chunk is",self.chunk[:100])
        c.save([key, self.chunk])

class CustomStorage(ABC):
    
    @abstractmethod
    def get(self):
        pass
    @abstractmethod
    def save(self,data):
        pass
    
    def utf8len(self,s:str):        
        return str(len(s.encode('utf-8')))  
    
    def parseGetResponse(self,key,value):
        command = ''        
        if value !=None:            
            command = "VALUE" + " " + key +" 0 "+self.utf8len(value) + NEWLINESEPERATOR + value + NEWLINESEPERATOR + "END\r\n"
            # command = value
        else:
            command = "VALUE" + " " + key +" 0 "+self.utf8len(NOTEXISTS.decode()) + NEWLINESEPERATOR + NOTEXISTS.decode() + NEWLINESEPERATOR + "END\r\n"
            # command = NOTEXISTS
        return command    


        
        
class GoogleFireStore(CustomStorage):
    
    def __init__(self):        
        self.load()
    
    def load(self):
        self.db = firestore.Client(project=projectId).collection(u'items')
        return self.db
    
    def save(self,data):
        
        doc_ref = self.db.document(f'{data[0]}')
        doc_ref.set({
            "key": data[0],
            "value":data[1]
        })
        print("Data saved to firestore successfully")
    
    def get(self,key):
        doc_ref = self.db.document(u''+key)
        doc = doc_ref.get()
        if doc.exists:
            print(doc.to_dict())
            return self.parseGetResponse(key,doc.to_dict()["value"])                        
        else:
            return self.parseGetResponse(key,None)
    
    

# g = GoogleFireStore()
# f = FileHandler("data/book.txt",noOfMappers)
# for i in range(noOfMappers):
#     f.sendNextChunkToKeyValueServer("Mapper-"+str(i),g)
# print(g.get("Mapper-4"))    

