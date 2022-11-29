from collections import Counter, defaultdict
import socket
from time import sleep
from putDataInKeyValueStore import GoogleFireStore
from client import Client
from constants import STOPWORDS
from server import Server
from util import read_ini
import re
import os
import sys

os.environ["PYTHONHASHSEED"] = "0"

config = read_ini("config.ini")




def word_count_m(start, data):
    data = data.lower()
    data = re.sub(r"[^\w\s]", "", data).split()
    return Counter(data)


def inverted_index_m(start, data):
    data = data.lower()
    data = re.sub(r"[^\w\s]", "", data).split("\n")
    start = int(start)
    d = defaultdict(set)
    for i, sentence in enumerate(data):
        for word in sentence.split():
            if word not in STOPWORDS:
                d[word].add(start + i)
    return d


class Mapper:
    def __init__(self, port, index,reducerInternalIps,host=socket.gethostbyname(socket.gethostname())):

        self.port = port
        self.host = host
        self.reducerHosts = reducerInternalIps
        self.mapperServers = []
        self.index = index
        self.g = GoogleFireStore()
        self.reducerIps = self.g.get("reducerinternalips").split()
        self.noOfReducers = len(self.reducerIps)

    def mapperWork(self, func, start, name):
        # When ever mapper receives a request, start the mapper process
        print(f"[{name}] Mapper received a request", start, name)
        # Get the data from the keyvalue server
        sleep(1)
        mapperData = self.g.get(name)
        print(f"[{name}] Mapper got the data from key-value server")
        # Do mapper work
        keyValueGenerated = eval(f'{func}("{start}","""{mapperData}""")')        
        # For each key,value -- Send it to appropriate reducer
        count = 0
        for key, value in keyValueGenerated.items():
            # Adding sleep to maintain some consistency
            sleep(0.001)
            targetReducer = len(key) % self.noOfReducers            
            toReducerClient = Client(self.reducerIps[targetReducer],8080)
            toReducerClient.append(key, str(value))        
            count +=1
            if count == 10:
                break
        print(f"[{name}] Completed its work, sending ACK to master")
        # Once the mapper sent the data, send an ACK to master that it has done its work
        # toMasterClient = Client(self.host, int(config["MASTER"]["PORT"]))
        # # For the ith mapper, it will send mapper-i, done message to the master
        # toMasterClient.set(name, "Done")

    def startMapper(self, func):
        print("Starting mappers")

        
        s = Server(self.host, self.port)
        self.mapperWork(func, self.index, "Mapper-" + str(self.index))
        # proc = s.startServerOnADifferentProcess(
        #     self.mapperWork, args=(func, chunkSize * self.index, "Mapper-" + str(self.index)), name=f"Mapper-" + str(self.index)
        # )
        
        
# m = Mapper(3,3,8080,1)
# m.startMapper("inverted_index_m")
        
