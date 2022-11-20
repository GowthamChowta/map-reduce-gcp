from collections import Counter, defaultdict
import socket
from time import sleep
from client import Client
from constants import STOPWORDS
from server import Server
from util import read_ini
import re
import os

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
    def __init__(self, noOfMappers, noOfReducers, port, host=socket.gethostbyname(socket.gethostname())):

        self.noOfMappers = noOfMappers
        self.noOfReducers = noOfReducers
        self.port = port
        self.host = host
        self.mapperServers = []

    def mapperWork(self, socket, func, start, name):
        # When ever mapper receives a request, start the mapper process
        print(f"[{name}] Mapper received a request", start, name)
        # Get the data from the keyvalue server
        sleep(1)
        c = Client(self.host, int(config["KEYVALUE"]["PORT"]))
        mapperData = c.get(name)
        print(f"[{name}] Mapper got the data from key-value server")
        # Do mapper work
        keyValueGenerated = eval(f'{func}("{start}","""{mapperData}""")')
        # For each key,value -- Send it to appropriate reducer
        for key, value in keyValueGenerated.items():
            # Adding sleep to maintain some consistency
            sleep(0.001)
            targetReducerPort = hash(key) % self.noOfReducers
            toReducerClient = Client(self.host, int(config["REDUCER"]["PORT"]) + targetReducerPort)
            toReducerClient.append(key, str(value))
        socket.send(f"Done\n{name}".encode())
        print(f"[{name}] Completed its work, sending ACK to master")
        # Once the mapper sent the data, send an ACK to master that it has done its work
        toMasterClient = Client(self.host, int(config["MASTER"]["PORT"]))
        # For the ith mapper, it will send mapper-i, done message to the master
        toMasterClient.set(name, "Done")

    def startMapper(self, func, chunkSize):
        print("Starting mappers")

        for i in range(self.noOfMappers):
            s = Server(self.host, self.port + i)
            proc = s.startServerOnADifferentProcess(
                self.mapperWork, args=(func, chunkSize * i, "Mapper-" + str(i)), name=f"Mapper-" + str(i)
            )
            self.mapperServers.append(proc)
