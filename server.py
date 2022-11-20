import socket
from threading import Thread
import multiprocessing


class Server:
    """Starts a new TCP server
    * For every new incoming connection, it will create new process for execution.
    """

    def __init__(self, host, port=8080):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.port = port
        self.socket.bind((host, port))
        self.index = 0
        self.parallelProcess = None

    def startServer(self, work, args=(), name=""):
        print(f"Starting server {name} on {self.port}")
        self.socket.listen(5)
        while True:
            client_sock, client_addr = self.socket.accept()

            parallelConnection = DoInParallelWithThreading(work, args=(client_sock,) + args, name=f"t{self.index}")
            parallelConnection.getThread().start()
            self.index += 1

    def startServerOnADifferentProcess(self, work, args=(), name=""):
        p = multiprocessing.Process(target=self.startServer, args=(work, args, name), name=name)
        p.start()


class DoInParallelWithMultiProcessing:
    def __init__(self, work, args=(), name=""):
        self.process = multiprocessing.Process(target=work)

    def getProcess(self):
        return self.process

    def startProcess(self):
        self.process.start()


class DoInParallelWithThreading:
    def __init__(self, work, args=(), name=""):
        self.__thread = Thread(target=work, args=args, name=name)

    def getThread(self):
        return self.__thread

    def start(self):
        self.__thread.start()
