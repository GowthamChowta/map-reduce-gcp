import socket
from time import sleep

from util import read_ini


class Client:
    def __init__(self, host, port=8080):
        self.host = host
        self.port = port
        self.createClientFunc = self.createTCPClient

    def createTCPClient(self):
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect((self.host, self.port))

    def utf8len(self, s):
        return str(len(s.encode("utf-8")))

    def sendMessageToServer(self, command):
        # print("[Client]:", command)
        self.client.send(command.encode())

    def receiveACKMessageFromServer(self):
        messageFromServer = self.client.recv(2048 * 2048)
        # print("[Server]:", messageFromServer.decode())
        return messageFromServer.decode()

    def createSendReceiveMessageFromServer(self, command):
        # 1. Creates a TCP/Memcached client
        self.createClientFunc()
        # 2. Sends message to server
        self.sendMessageToServer(command)
        # 3. Recevives ACK message from server
        return self.receiveACKMessageFromServer()

    def createSendMessageFromServer(self, command):
        # 1. Creates a TCP/Memcached client
        self.createClientFunc()
        # 2. Sends message to server
        self.sendMessageToServer(command)

    def set(self, key, value):
        command = "set" + " " + key + " " + self.utf8len(value) + " 0 " + "\r\n" + value + "\r\n"
        self.createSendReceiveMessageFromServer(command)

    def set2(self, key, value):
        command = "set" + " " + key + " " + self.utf8len(value) + " 0 " + "\r\n" + value + "\r\n"
        self.createSendMessageFromServer(command)

    def get(self, key):
        command = "get" + " " + key + "\r\n"
        ack = self.createSendReceiveMessageFromServer(command)
        try:
            resp = ack.split("\n", 1)[1][:-3]
        except:

            resp = "test"
            print("*" * 200)
            print("I am here", key)
            print("*" * 200)
        return resp

    def append(self, key, value):
        command = "append" + " " + key + " " + self.utf8len(value) + " 0 " + "\r\n" + value + "\r\n"
        self.createSendMessageFromServer(command)

    def startReducer(self, key, value):
        command = "startReducer" + " " + key + " " + self.utf8len(value) + " 0 " + "\r\n" + value + "\r\n"
        self.createSendMessageFromServer(command)

    def startMapperRequest(self):

        self.get("start Mapper")

    def dummyRequest(self):

        self.get("test3")
