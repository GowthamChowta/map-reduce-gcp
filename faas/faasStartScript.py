from threading import Thread
import requests
from utils.util import read_ini


params = read_ini()

noOfMappers = int(params["APPLICATION"]["noofmappers"])
noOfReducers = int(params["APPLICATION"]["noofreducers"])


mapperFunctionInvokeUrl = params.get("FAAS","mapperFunctionInvokeUrl")
reducerFunctionInvokeUrl = params.get("FAAS","reducerFunctionInvokeUrl")
    
def doReducerWork(params):
    x = requests.get(reducerFunctionInvokeUrl,params=params)    

def doMapperWork(params):
    x = requests.get(mapperFunctionInvokeUrl,params=params)
    
mapperThreads = []
for i in range(noOfMappers):
    p = {
        "mapperId": str(i)
    }
    print(f"[Mapper-{i}] Starting mapper work")
    t = Thread(target=doMapperWork,args=(p,),name=f"Mapper-{i}")
    mapperThreads.append(t)
    t.start()

# Wait for all mappers to complete
for t in mapperThreads:
    t.join()

print(f"[Mappers] Completed work!")

reducerThreads = []
for i in range(noOfReducers):
    p = {
        "reducerId": str(i)
    }
    print(f"[Reducer-{i}] Starting reducer work")
    t = Thread(target=doReducerWork,args=(p,),name=f"Reducer-{i}")
    reducerThreads.append(t)
    t.start()

# Wait for all reducers to complete
for t in reducerThreads:
    t.join() 

print(f"[Reducers] Completed work!")



