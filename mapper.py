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
    def __init__(self, port, index,host=socket.gethostbyname(socket.gethostname())):

        self.port = port
        self.host = host
        self.mapperServers = []
        self.index = index
        self.g = GoogleFireStore()
        self.reducerIps = self.g.getOriginal("reducerinternalips").split()
        self.noOfReducers = len(self.reducerIps)

    def mapperWork(self, func, start, name):
        # When ever mapper receives a request, start the mapper process
        print(f"[{name}] Mapper received a request", start, name)
        # Get the data from the keyvalue server
        mapperData = self.g.getOriginal(name)
        print(f"[{name}] Mapper got the data from key-value server")
        # Do mapper work
        keyValueGenerated = eval(f'{func}("{start}","""{mapperData}""")')        
        # For each key,value -- Send it to appropriate reducer
        count = 0
        print("What is reducer ips",self.reducerIps)
        print("key value items", list(keyValueGenerated.items())[:10])
        for key, value in keyValueGenerated.items():
            # Adding sleep to maintain some consistency
            sleep(0.1)
            targetReducer = len(key) % self.noOfReducers            
            print("Sending data to reducer ",targetReducer, self.reducerIps[targetReducer])
            toReducerClient = Client(self.reducerIps[targetReducer],8080)
            toReducerClient.append(key, str(value))        
            count +=1
            if count == 10:
                break
        print(f"[{name}] Completed its work, sending ACK to master")
        # Once the mapper sent the data, send an ACK to master that it has done its work
        masterHost = self.g.getOriginal("masterinternalip")
        print("master host is",masterHost)
        toMasterClient = Client(masterHost, 8080)
        # # For the ith mapper, it will send mapper-i, done message to the master
        toMasterClient.set(name, "Done")

    def startMapper(self, func):
        print("Starting mappers")

        
        s = Server(self.host, self.port)
        self.mapperWork(func, self.index, "Mapper-" + str(self.index))
        # proc = s.startServerOnADifferentProcess(
        #     self.mapperWork, args=(func, chunkSize * self.index, "Mapper-" + str(self.index)), name=f"Mapper-" + str(self.index)
        # )
        

index = int(sys.argv[1])

m = Mapper(8080,index)
m.startMapper("inverted_index_m")
        
