import json
from abc import ABC, abstractmethod
from collections import defaultdict


class Storage(ABC):
    def __init__(self):
        self.keyValueStore = defaultdict(int)

    @abstractmethod
    def loadKeyValueStore():
        pass

    def getKeyValueStore(self):
        return self.loadKeyValueStore()

    @abstractmethod
    def writeToDisk(self):
        pass


class SaveLoadDisk(Storage):

    # # static variable
    def __init__(self):
        super().__init__()
        self.keyValueStore = self.loadKeyValueStore()

    def loadKeyValueStore(self):
        """Function to load the JSON file when the server starts"""
        with open("data.json", "r") as f:
            try:
                return json.load(f)
            except:
                return defaultdict(int)

    def writeToDisk(self, newData):
        self.keyValueStore = self.loadKeyValueStore()
        self.keyValueStore.update(newData)
        with open("data.json", "w") as f:
            json.dump(self.keyValueStore, f)
        print(f"Writing data to disk Complete")


class SaveToMemory(Storage):
    def __init__(self):
        super().__init__()
        self.keyValueStore = defaultdict(int)

    def loadKeyValueStore(self):
        return self.keyValueStore

    def writeToDisk(self, newData):
        pass
