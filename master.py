import argparse
import multiprocessing
import os
import socket
from threading import Thread
from time import sleep, time
from putDataInKeyValueStore import GoogleFireStore

from instancemanagement import installDependenciesOnMachine, sendDefaultApplicationCredentialsFileToMachine, setupMachine
from client import Client
from constants import INVALIDCOMMAND
from server import Server
from util import read_ini
import re

config = read_ini("config.ini")



class Master:
    def __init__(self, noOfMappers, noOfReducers, task, dataDir):
        self.noOfMappers = noOfMappers
        self.noOfReducers = noOfReducers
        self.host = socket.gethostbyname(socket.gethostname())
        self.task = task
        # self.mapper = Mapper(self.noOfMappers, self.noOfReducers, int(config["MAPPER"]["PORT"]), mappersIP, reducersIP)
        # self.reducer = Reducer(self.noOfReducers, int(config["REDUCER"]["PORT"]), reducersIP)
        # self.master = Server(self.host, int(config["MASTER"]["PORT"]))
        # self.keyValue = KeyValueServer(SaveLoadDisk())
        self.completedMappers = []
        self.g = GoogleFireStore()        
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
            self.reducerIps = self.g.getOriginal("reducerinternalips").split()
            self.noOfReducers = len(self.reducerIps)
            for i in range(self.noOfReducers):
                print(f"[Master] Send request to Reducer[{i}] to start the task")
                toReducerClient = Client( self.reducerIps[i],8080)
                toReducerClient.startReducer("startReducer", "reducer")


    def startMappers(self, chunkSize):

        self.mapper.startMapper(self.getMapperFuncName(), chunkSize)

    def startReducers(self):
        self.reducer.startReducer(self.getReducerFuncName())
        
        
def setupInfrastructure(noOfMappers, noOfReducers):
    

    masterPublicIp, masterInternalIp = setupMachine("master-2")    
    
    # masterPublicIp = "34.94.160.168"    
        
    mappersIP = []
    reducersIP = []
    for i in range(noOfMappers):
        mapperPublicIP, mapperInternalIP = setupMachine("mapperttest-"+str(i))
        mappersIP.append((mapperPublicIP, mapperInternalIP))        
    # mappersIP = [["34.102.112.111"],["35.235.93.87"],["34.102.27.112"]]

    for i in range(noOfReducers):
        reducerPublicIP, reducerInternalIP = setupMachine("reducer-"+str(i))
        reducersIP.append((reducerPublicIP, reducerInternalIP))
    
    config.set("GCP","masterpublicip",masterPublicIp)
    config.set("GCP","masterinternalip",masterInternalIp)
    
    config.set("GCP","mapperpublicips"," ".join([i[0] for i in mappersIP]))
    config.set("GCP","mapperinternalips"," ".join([i[1] for i in mappersIP]))
    
    config.set("GCP","reducerpublicips"," ".join([i[0] for i in reducersIP]))
    config.set("GCP","reducerinternalips"," ".join([i[1] for i in reducersIP]))
    
    with open('config.ini', 'w') as configfile:    # save
        config.write(configfile)    
        
    print("[Main] Setting the ips to google firestore")
    g = GoogleFireStore()
    g.save(["mapperpublicips",config.get("GCP","masterpublicip")])
    g.save(["masterinternalip",config.get("GCP","masterinternalip")])
    g.save(["mapperpublicips",config.get("GCP","mapperpublicips")])
    g.save(["mapperinternalips",config.get("GCP","mapperinternalips")])
    g.save(["reducerpublicips",config.get("GCP","reducerpublicips")])
    g.save(["reducerinternalips",config.get("GCP","reducerinternalips")])    
    print("[Main] Updated the ip address information to google firestore")
    

def installDependencies():
    
    commandsToSetupOnMachine = [
        "sudo apt install -y git",
        "git clone https://github.com/GowthamChowta/map-reduce-gcp.git",
        "sudo apt install -y python3-pip",    
        "sudo pip install google-cloud-core",
        "sudo pip install google-cloud-compute" ,
        "sudo pip install google-cloud-firestore",
        "sudo pip install google-cloud-storage",
        "sudo pip install paramiko"
    ]
    masterPublicIp = config.get("GCP", "masterpublicip")
    installDependenciesOnMachine(masterPublicIp, commandsToSetupOnMachine)
    sendDefaultApplicationCredentialsFileToMachine(masterPublicIp)
    print("[Main] Master setup is complete")
    
    mappersIP = config.get("GCP","mapperpublicips").split()
    
    for i in range(len(mappersIP)):
        print(f"Sending credentials to {mappersIP[i]} ...")        
        sendDefaultApplicationCredentialsFileToMachine(mappersIP[i])
        print(f"Sending credentials to {mappersIP[i]} done")
        
    for i in range(len(mappersIP)):
        print(f"Installing dependencies on {mappersIP[i]} ...")                
        installDependenciesOnMachine(mappersIP[i],commandsToSetupOnMachine)
        print(f"Installing dependencies on {mappersIP[i]} done")        
    
    print("[Main] Mapper setup is complete")
    
    reducersIP = config.get("GCP","reducerpublicips").split()
    
    for i in range(len(reducersIP)):
        print(f"Sending credentials to {reducersIP[i]} ...")        
        sendDefaultApplicationCredentialsFileToMachine(reducersIP[i])
        print(f"Sending credentials to {reducersIP[i]} done")
        
    for i in range(len(reducersIP)):
        print(f"Installing dependencies on {reducersIP[i]} ...")                
        installDependenciesOnMachine(reducersIP[i],commandsToSetupOnMachine)
        print(f"Installing dependencies on {reducersIP[i]} done") 
        
        
    print("[Main] Reducer setup is complete")


def startMapperWork():
    commandsToRun = [
        "python3 map-reduce-gcp/mapper.py "
    ]
    mapperIps = config.get("GCP","mapperinternalips").split()
    for i in range(len(mapperIps)):
        installDependenciesOnMachine(mapperIps[i], [commandsToRun[0] + str(i)])
    

def startReducerServers():
    commandsToRun = [
        "python3 map-reduce-gcp/reducer.py "
    ]
    reducerIps = config.get("GCP","reducerinternalips").split()
    for i in range(len(reducerIps)):
        installDependenciesOnMachine(reducerIps[i],[commandsToRun[0] + str(i)])

    


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
    m = Master(noOfMappers, noOfReducers, task, dataDir)
    
    # s = Server(socket.gethostbyname(socket.gethostname()))
    # # Args not necessary. 
    # s.startServerOnADifferentProcess(m.masterDoWork,args=('Master',),name="Master Server")
    
    # setupInfrastructure(noOfMappers, noOfReducers)
    print("Setting up infrastructure")
    # sleep(30)
    print("Installing dependencies")
    # installDependencies()
    startReducerServers()
    print("[Main] Reducer servers started")
    sleep(5)
    startMapperWork()