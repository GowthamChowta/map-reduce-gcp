from time import sleep
import os
import threading
from google.api_core.extended_operation import ExtendedOperation

from gcpPythonClientHelper import create_instance, disk_from_image, getInstanceExternalInternalIpByName, read_ini, runCommandsOnAMachineOverSSH, setupMachineByhostIP
import sys
from google.cloud import storage



STORAGE = sys.argv[1]

params = read_ini()

applicationCredentialsPath = params["GCP"]["localappcredentialsPath"]
projectPath = params["GCP"]["projectPath"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = applicationCredentialsPath
ZONE= params["USER"]["ZONE"]
PROJECTID=params["USER"]["PROJECTID"]
USER =params["USER"]["USERNAME"]
SSHFilePath = params["USER"]["SSHFilePath"]
SERVERPORT =params["USER"]["SERVERPORT"]
image_name=params["GCP"]["imageName"]
source_image=params["GCP"]["sourceImage"]
disk_type=f'zones/{ZONE}/diskTypes/pd-balanced'




commandsToSetupOnMachine = [
    "sudo apt install -y git",
    "git clone https://github.com/GowthamChowta/map-reduce-gcp.git",
    "sudo apt install -y python3-pip",    
    "sudo pip install google-cloud-core",
    "sudo pip install google-cloud-compute" ,
    "sudo pip install google-cloud-firestore"
]
commandsToServer = [
        f"python3 -u simple-keyvaluestore/server.py {SERVERPORT} {STORAGE}"
    ]
# commandsToSetupOnClient = [
#     "sudo apt install -y git",
#     "git clone https://github.com/GowthamChowta/simple-keyvaluestore.git"    
# ]


def sendDefaultApplicationCredentialsFileToMachine(machinePublicIP):
    print("Sending application credentials to the machine for machine public IP",machinePublicIP)
    ssh = setupMachineByhostIP(machinePublicIP)
    ftp_client=ssh.open_sftp()
    ftp_client.put(applicationCredentialsPath, f"/home/{USER}/cred.json")    
    ftp_client.close()
    

def setupMachine(instance_name,machine_type="e2-micro"):

    
    print("Creating machine")
    ## Starting server machine
    boot_disk = disk_from_image(disk_type,10,True,source_image=source_image)
    create_instance(project_id=PROJECTID,zone=ZONE,instance_name=instance_name,disks=[boot_disk],machine_type=machine_type,external_access=True)
    machinePublicIP,machineInternalIP = getInstanceExternalInternalIpByName(instance_name)
    # sendDefaultApplicationCredentialsFileToMachine(machinePublicIP)
    print(f"machine Public IP address is {machinePublicIP}")
    print(f"Removing known hosts if exists for {machinePublicIP}")
    os.system(f"ssh-keygen -R {machinePublicIP}")
    return machinePublicIP, machineInternalIP

# def setupClient():    
#     ## Starting client machine
#     print("Starting Client machine")
#     create_instance(project_id=PROJECTID,zone=ZONE,instance_name=machine_names[1],disks=[boot_disk_client],machine_type="e2-micro",external_access=True)
#     clientPublicIP, clientInternalIP = getInstanceExternalInternalIpByName(machine_names[1])
#     print(f"Client Public IP address is {clientPublicIP}")
#     print(f"Removing known hosts if exists for {clientPublicIP}")
#     os.system(f"ssh-keygen -R {clientPublicIP}")
#     return clientPublicIP, clientInternalIP




def installDependenciesOnMachine(machinePublicIP,commands):
    print("Installing dependencies on machine ",machinePublicIP)
    ssh = setupMachineByhostIP(machinePublicIP)
    ## Copying the cred.json file to the machine
    # runCommandsOnAMachineOverSSH(ssh,commandsToSetupOnMachine)
    ## Start Machine process
    t = threading.Thread(target=runCommandsOnAMachineOverSSH,args=(ssh,commands))
    t.start()    
    

# def installDependenciesOnClient(clientPublicIP, serverInternalIP):
#     sshClient = setupMachineByhostIP(clientPublicIP)
#     runCommandsOnAMachineOverSSH(sshClient,commandsToSetupOnClient)
#     commandsToClient = [
#         f"python3 -u simple-keyvaluestore/client.py {SERVERPORT} {serverInternalIP}"
#     ]
#     runCommandsOnAMachineOverSSH(sshClient,commandsToClient)
#     print("Client request successfull")


# serverPublicIP, serverInternalIP = setupServer()
# clientPublicIP, clientInternalIP = setupClient()
# sendDefaultApplicationCredentialsFileToServer(serverPublicIP)
# installDependenciesOnServer(serverPublicIP)
# installDependenciesOnClient(clientPublicIP, serverInternalIP)
# print("Completed key-value store")

# delete_instance(project_id=PROJECTID, zone=ZONE,machine_name=machine_names[0])
# delete_instance(project_id=PROJECTID, zone=ZONE,machine_name=machine_names[1])
# delete_bucket(storage, bucket_name=bucketName)



