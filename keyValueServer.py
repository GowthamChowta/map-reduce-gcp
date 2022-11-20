import random
import re
from time import sleep
import json
from constants import INVALIDCOMMAND, NEWLINESEPERATOR, NOTEXISTS, STORED
from saveLoadDisk import SaveLoadDisk, Storage


class KeyValueServer:
    """
    Does the actual work.
    Parses the message from client and sends appropriate response back.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.keyValueStore = self.storage.getKeyValueStore()

    def validateAndParseMessage(self, message):
        message = message.decode()
        data = re.split(" |\r\n", message)[:-1]
        if len(data) == 0:
            return False

        if len(data) == 2 and data[0] == "get":
            self.data = data
        elif data[0] == "set":
            self.data = data
        else:
            return False
        return True

    def doWork(self, clientSock, name):

        self.keyValueStore = self.storage.getKeyValueStore()
        self.clientSock = clientSock
        messageFromClient = self.clientSock.recv(2048 * 2048)

        isValid = self.validateAndParseMessage(messageFromClient)
        if not isValid:
            self.sendMessageToClient(INVALIDCOMMAND)
            return

        if self.data[0] == "get":
            _, key = self.data
            self.sendMessageToClient(self.parseGetResponse(key))

        elif self.data[0] == "set":
            key = self.data[1]
            value = self.data[5:]
            value = " ".join(value)
            self.keyValueStore[key] = value
            self.storage.writeToDisk({key: value})
            self.sendMessageToClient(STORED)

    def sendMessageToClient(self, message):
        if type(message) == str:
            message = message.encode()
        self.clientSock.send(message)

    def utf8len(self, s):
        return str(len(s.encode("utf-8")))

    def parseGetResponse(self, key):
        command = ""
        if key in self.keyValueStore:
            value = self.keyValueStore[key]
            command = (
                "VALUE"
                + " "
                + key
                + " 0 "
                + self.utf8len(value)
                + NEWLINESEPERATOR
                + value
                + NEWLINESEPERATOR
                + "END\r\n"
            )
        else:
            command = (
                "VALUE"
                + " "
                + key
                + " 0 "
                + self.utf8len(NOTEXISTS.decode())
                + NEWLINESEPERATOR
                + NOTEXISTS.decode()
                + NEWLINESEPERATOR
                + "END\r\n"
            )
        return command

    def randomDelay(self):
        sleep(random.randint(1, 2))
        pass

    def saveDataToDir(self, dict, filePath):
        with open("output/" + filePath + ".json", "w") as f:
            json.dump(dict, f)
        print(f"[{filePath}] Data saved to file")
