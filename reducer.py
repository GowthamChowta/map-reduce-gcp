from collections import defaultdict
import socket
from putDataInKeyValueStore import GoogleFireStore


from server import Server

import socket
import re
from time import sleep
import json
from constants import INVALIDCOMMAND, NEWLINESEPERATOR, NOTEXISTS, STORED
import sys
# HOST = socket.gethostbyname(socket.gethostname())


def word_count_r(keyValueStore, key, value):
    if key not in keyValueStore:
        keyValueStore[key] = int(value)
    else:
        keyValueStore[key] += int(value)
    # return keyValueStore


def inverted_index_r(keyValueStore, key, value):
    value = set([word.strip() for word in value[1:-1].split(",")])
    if key not in keyValueStore:
        keyValueStore[key] = value
    else:
        try:
            keyValueStore[key].update(value)
        except Exception as e:
            print("IN exception", key, value, type(key), type(value))
            # keyValueStore[key].add(value)


# return keyValueStore


class ReducerKeyValueServer:
    """
    Does the actual work.
    Parses the message from client and sends appropriate response back.
    """

    def __init__(self):
        self.keyValueStore = defaultdict()
        self.g = GoogleFireStore()

    def validateAndParseMessage(self, message):
        message = message.decode()
        data = re.split(" |\r\n", message)[:-1]
        # Get array message should be of length 2
        if len(data) == 0:
            return False

        elif data[0] == "append" or data[0] == "startReducer":
            self.data = data
        else:
            return False
        return True

    def doWork(self, clientSock, func, name):

        messageFromClient = clientSock.recv(2048 * 2048)

        # print("[KeyValueMSGFromClient]:", messageFromClient)
        isValid = self.validateAndParseMessage(messageFromClient)
        if not isValid:
            clientSock.send(INVALIDCOMMAND)
            return

        elif self.data[0] == "append":
            key = self.data[1]
            value = self.data[5:]
            value = " ".join(value)
            eval(f'{func}(self.keyValueStore,"{key}", "{value}")')            
            clientSock.send(STORED)
            print("Message received for", key)
            # self.saveDataToDir(self.keyValueStore, name)

        elif self.data[0] == "startReducer":
            print(f"Starting {name}")
            self.saveDataToDir(self.keyValueStore, name)
            self.saveDataToFireStore(name)
            clientSock.send(STORED)
        # print("[KeyValueServer]:", message)

    def saveDataToDir(self, dict, filePath):
        # TODO need to change this
        for key, value in dict.items():
            if type(value) == int:
                break
            dict[key] = list(value)
        with open("/home/chgowt_iu_edu/map-reduce-gcp/output/" + filePath + ".json", "w") as f:
            json.dump(dict, f)
        print(f"[{filePath}] Data saved to disk")
    
    def saveDataToFireStore(self,name):
        for key, value in self.keyValueStore.items():
            self.g.save([key,value])
        print(f"Data from {name} saved successfully to firestore")
        


class Reducer:
    def __init__(self, noOfReducers, port,index, host=socket.gethostbyname(socket.gethostname())):

        self.noOfReducers = noOfReducers
        self.port = port
        self.host = host
        self.index = index

    def startReducer(self, func):
        print("Starting Reducers")        
        s = Server(self.host, self.port)
        s.startServerOnADifferentProcess(
            ReducerKeyValueServer().doWork,
            args=(
                func,
                f"Reducer-" + str(self.index),
            ),
            name=f"Reducer-" + str(self.index),
        )

index = int(sys.argv[1])

r = Reducer(-1,8080,index)
r.startReducer("inverted_index_r")