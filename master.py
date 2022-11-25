import argparse
import chunk
import multiprocessing
import os
import socket
from threading import Thread
from time import sleep, time

from instancemanagement import installDependenciesOnMachine, sendDefaultApplicationCredentialsFileToMachine, setupMachine
from client import Client
from constants import INVALIDCOMMAND
from keyValueServer import KeyValueServer
from mapper import Mapper
from reducer import Reducer
from saveLoadDisk import SaveLoadDisk
from server import Server
from util import read_ini
import re

config = read_ini("config.ini")



class Master:
    def __init__(self, noOfMappers, noOfReducers, task, dataDir, mappersIP, reducersIP):
        self.noOfMappers = noOfMappers
        self.noOfReducers = noOfReducers
        self.host = socket.gethostbyname(socket.gethostname())
        self.task = task
        # self.mapper = Mapper(self.noOfMappers, self.noOfReducers, int(config["MAPPER"]["PORT"]), mappersIP, reducersIP)
        # self.reducer = Reducer(self.noOfReducers, int(config["REDUCER"]["PORT"]), reducersIP)
        # self.master = Server(self.host, int(config["MASTER"]["PORT"]))
        # self.keyValue = KeyValueServer(SaveLoadDisk())
        # self.completedMappers = []
        # self.master.startServerOnADifferentProcess(self.masterDoWork, args=("abc",), name="master")

    def getMapperFuncName(self):
        if self.task == "wc":
            return "word_count_m"
        elif self.task == "inv_ind":
            return "inverted_index_m"

    def getReducerFuncName(self):
        if self.task == "wc":
            return "word_count_r"
        elif self.task == "inv_ind":
            return "inverted_index_r"

    def masterDoWork(self, socket, args):
        msgToMaster = socket.recv(2048)
        msgToMaster = msgToMaster.decode()
        msgToMaster = re.split(" |\r\n", msgToMaster)[:-1]
        if msgToMaster[0] == "set":
            key = msgToMaster[1]
            self.completedMappers.append(key)
            print("Completed Mappers:", self.completedMappers)
            socket.send(f"Master ACK {self.completedMappers}".encode())
        else:
            socket.send(INVALIDCOMMAND)
        if len(self.completedMappers) >= self.noOfMappers:
            print("All mappers ACK, tell the reducers to start the task")
            for i in range(self.noOfReducers):
                print(f"[Master] Send request to Reducer[{i}] to start the task")
                toReducerClient = Client(self.host, int(config["REDUCER"]["PORT"]) + i)
                toReducerClient.startReducer("startReducer", "reducer")

    def startKeyValue(self):
        self.keyValueServer = Server(self.host, int(config["KEYVALUE"]["PORT"]))
        self.keyValueServer.startServerOnADifferentProcess(
            self.keyValue.doWork, args=("Keyvalue server",), name="keyValueServer"
        )

    def startMappers(self, chunkSize):

        self.mapper.startMapper(self.getMapperFuncName(), chunkSize)

    def startReducers(self):
        self.reducer.startReducer(self.getReducerFuncName())


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Map reducer kwargs")
    parser.add_argument("--noofmappers", dest="noOfMappers", type=int, help="Number of mappers to spawn")
    parser.add_argument("--noofreducers", dest="noOfReducers", type=int, help="Number of reducers to spawn")
    parser.add_argument("--task", dest="task", choices=["wc", "inv_ind"], help="wc and inv_ind are only valid now")
    parser.add_argument("--datadir", dest="DATA_DIR", default="Sample data", help="Data directory path")

    args = parser.parse_args()
    task = args.task
    noOfMappers = args.noOfMappers
    noOfReducers = args.noOfReducers
    dataDir = args.DATA_DIR

    # masterPublicIp, masterInternalIp = setupMachine("master-2")
    # keyValueServerPublicIp, keyValueServerInternalIp = setupMachine("keyvalueserver-2")
    mappersIP = []
    reducersIP = []
    # for i in range(noOfMappers):
    #     mapperPublicIP, mapperInternalIP = setupMachine("mapperttest-"+str(i))
    #     mappersIP.append((mapperPublicIP, mapperInternalIP))        
    mappersIP = [["34.102.112.111"],["35.235.93.87"],["34.102.27.112"]]
    for i in range(noOfMappers):
        print(f"Sending credentials to {mappersIP[i][0]} ...")        
        # sendDefaultApplicationCredentialsFileToMachine(mappersIP[i][0])
        print(f"Sending credentials to {mappersIP[i][0]} done")
        
    commandsToSetupOnMachine = [
        "sudo apt install -y git",
        "git clone https://github.com/GowthamChowta/map-reduce-gcp.git",
        "sudo apt install -y python3-pip",    
        "sudo pip install google-cloud-core",
        "sudo pip install google-cloud-compute" ,
        "sudo pip install google-cloud-firestore"
    ]
    
    for i in range(noOfMappers):
        print(f"Installing dependencies on {mappersIP[i][0]} ...")                
        # installDependenciesOnMachine(mappersIP[i][0],commandsToSetupOnMachine)
        print(f"Installing dependencies on {mappersIP[i][0]} done")        
        
    # for i in range(noOfReducers):
    #     reducerPublicIP, reducerInternalIP = setupMachine("reducer-"+str(i))
    #     reducersIP.append((reducerPublicIP, reducerInternalIP))
        
    # masterNode = Master(noOfMappers, noOfReducers, task, dataDir, mappersIP, reducersIP)
    
    # masterNode.startKeyValue()
    # masterNode.startMappers(chunkSize)
    # masterNode.startReducers()

    # for i in range(noOfMappers):
    #     print("Reading next chunk", "*" * 20)
    #     f.sendNextChunkToKeyValueServer("Mapper-" + str(i))
    # print("Centralized key-value store is updated!!")
    # sleep(1)
    # # Once the data is loaded in the key-value store --> tell the mappers to start the task
    # for i in range(noOfMappers):
    #     c = Client(socket.gethostbyname(socket.gethostname()), int(config["MAPPER"]["PORT"]) + i)
    #     p = Thread(target=c.startMapperRequest)
    #     sleep(0.5)
    #     p.start()
