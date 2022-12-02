from abc import ABC, abstractmethod
from google.cloud import firestore
from utils.constants import NEWLINESEPERATOR, NOTEXISTS
from utils.util import read_ini

# The `project` parameter is optional and represents which project the client
# will act on behalf of. If not supplied, the client falls back to the default
# project inferred from the environment.
import os
try:
    if "map-reduce-gcp"  not in os.getcwd():
        os.chdir("map-reduce-gcp/")
except:
    print("Running locally")

params = read_ini()

projectId = params["USER"]["PROJECTID"]



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
        
    
    def getOriginal(self,key):
        doc_ref = self.db.document(u''+key)
        doc = doc_ref.get()
        if doc.exists:
            # print(doc.to_dict())
            return doc.to_dict()["value"]
        else:
            return None
    
    def get(self,key):
        doc_ref = self.db.document(u''+key)
        doc = doc_ref.get()
        if doc.exists:
            # print(doc.to_dict())
            return self.parseGetResponse(key,doc.to_dict()["value"])                        
        else:
            return self.parseGetResponse(key,None)
    
    

 

