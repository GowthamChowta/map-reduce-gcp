import os
from instancemanagement import installDependenciesOnMachine, sendDefaultApplicationCredentialsFileToMachine, setupMachine
from storageHandler import GoogleFireStore
from utils.util import read_ini


try:
    if "map-reduce-gcp"  not in os.getcwd():
        os.chdir("map-reduce-gcp/")
except:
    print("Running locally")

config = read_ini("config.ini")

projectId = config["USER"]["PROJECTID"]
applicationCredentialsPath = config["GCP"]["localappcredentialsPath"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = applicationCredentialsPath


masterPublicIp, masterInternalIp = setupMachine("master11-2")    
print("[Main] Setting the ips to google firestore")
config.set("GCP","masterpublicip",masterPublicIp)
config.set("GCP","masterinternalip",masterInternalIp)
g = GoogleFireStore()
g.save(["masterpublicip",config.get("GCP","masterpublicip")])
g.save(["masterinternalip",config.get("GCP","masterinternalip")])
with open('config.ini', 'w') as configfile:    # save
    config.write(configfile)  
    
    
commandsToSetupOnMachine = [
        "sudo apt install -y git",
        "git clone https://github.com/GowthamChowta/map-reduce-gcp.git",
        "sudo apt install -y python3-pip",    
        "sudo pip install google-cloud-core",
        "sudo pip install google-cloud-compute" ,
        "sudo pip install google-cloud-firestore",
        "sudo pip install google-cloud-storage",
        "sudo pip install paramiko",
        "python3 map-reduce-gcp/master.py --noofmappers 3 --noofreducers 3 --task inv_ind --datadir ./data/book_sample.txt"
    ]
masterPublicIp = config.get("GCP", "masterpublicip")
installDependenciesOnMachine(masterPublicIp, commandsToSetupOnMachine)
print("[Main] Map-Reduce complete")
