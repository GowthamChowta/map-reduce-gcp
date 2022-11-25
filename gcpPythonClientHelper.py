import re
import sys
from time import sleep
from typing import Any, List
import paramiko
from google.api_core.extended_operation import ExtendedOperation
from google.cloud import compute_v1
import configparser
import os


# if "map-reduce-gcp"  not in os.getcwd():
#     os.chdir("map-reduce-gcp/")

def read_ini(file_path="./config.ini"):
    config = configparser.ConfigParser()
    config.read(file_path)    
    return config


params = read_ini()

applicationCredentialsPath = params["GCP"]["applicationCredentials"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = applicationCredentialsPath
ZONE= params["USER"]["ZONE"]
PROJECTID=params["USER"]["PROJECTID"]
USER =params["USER"]["USERNAME"]
SSHFilePath = params["USER"]["SSHFilePath"]
SERVERPORT =params["USER"]["SERVERPORT"]



# Ref: https://cloud.google.com/python/docs/reference/compute/latest
def disk_from_image(
    disk_type: str,
    disk_size_gb: int,
    boot: bool,
    source_image: str,
    auto_delete: bool = True,
) -> compute_v1.AttachedDisk:
   
    boot_disk = compute_v1.AttachedDisk()
    initialize_params = compute_v1.AttachedDiskInitializeParams()
    initialize_params.source_image = source_image
    initialize_params.disk_size_gb = disk_size_gb
    initialize_params.disk_type = disk_type
    boot_disk.initialize_params = initialize_params
    # Remember to set auto_delete to True if you want the disk to be deleted when you delete
    # your VM instance.
    boot_disk.auto_delete = auto_delete
    boot_disk.boot = boot
    return boot_disk

# Ref: https://cloud.google.com/python/docs/reference/compute/latest
def wait_for_extended_operation(
    operation: ExtendedOperation, verbose_name: str = "operation", timeout: int = 300
) -> Any:
    
    result = operation.result(timeout=timeout)

    if operation.error_code:
        print(
            f"Error during {verbose_name}: [Code: {operation.error_code}]: {operation.error_message}",
            file=sys.stderr,
            flush=True,
        )
        print(f"Operation ID: {operation.name}", file=sys.stderr, flush=True)
        raise operation.exception() or RuntimeError(operation.error_message)

    if operation.warnings:
        print(f"Warnings during {verbose_name}:\n", file=sys.stderr, flush=True)
        for warning in operation.warnings:
            print(f" - {warning.code}: {warning.message}", file=sys.stderr, flush=True)

    return result

# Ref: https://cloud.google.com/python/docs/reference/compute/latest
def create_instance(
    project_id: str,
    zone: str,
    instance_name: str,
    disks: List[compute_v1.AttachedDisk],
    machine_type: str = "n1-standard-1",
    network_link: str = "global/networks/default",
    subnetwork_link: str = None,
    internal_ip: str = None,
    external_access: bool = False,
    external_ipv4: str = None,    
    custom_hostname: str = None
) -> compute_v1.Instance:
    
    instance_client = compute_v1.InstancesClient()

    # Use the network interface provided in the network_link argument.
    network_interface = compute_v1.NetworkInterface()
    network_interface.name = network_link
    if subnetwork_link:
        network_interface.subnetwork = subnetwork_link

    if internal_ip:
        network_interface.network_i_p = internal_ip

    if external_access:
        access = compute_v1.AccessConfig()
        access.type_ = compute_v1.AccessConfig.Type.ONE_TO_ONE_NAT.name
        access.name = "External NAT"
        access.network_tier = access.NetworkTier.PREMIUM.name
        if external_ipv4:
            access.nat_i_p = external_ipv4
        network_interface.access_configs = [access]

    # Collect information into the Instance object.
    instance = compute_v1.Instance()
    instance.network_interfaces = [network_interface]
    instance.name = instance_name        
    instance.disks = disks
    instance.metadata = {"items":[{"key":"enable-oslogin","value":"TRUE"}]}
    
    if re.match(r"^zones/[a-z\d\-]+/machineTypes/[a-z\d\-]+$", machine_type):
        instance.machine_type = machine_type
    else:
        instance.machine_type = f"zones/{zone}/machineTypes/{machine_type}"
    

    if custom_hostname is not None:
        # Set the custom hostname for the instance
        instance.hostname = custom_hostname


    # Prepare the request to insert an instance.
    request = compute_v1.InsertInstanceRequest()
    request.zone = zone
    request.project = project_id
    request.instance_resource = instance

    # Wait for the create operation to complete.
    print(f"Creating the {instance_name} instance in {zone}...")

    operation = instance_client.insert(request=request)

    wait_for_extended_operation(operation, "instance creation")

    print(f"Instance {instance_name} created.")
    return instance_client.get(project=project_id, zone=zone, instance=instance_name)

# Ref: https://cloud.google.com/python/docs/reference/compute/latest
def delete_instance(project_id: str, zone: str, machine_name: str) -> None:

    instance_client = compute_v1.InstancesClient()

    print(f"Deleting {machine_name} from {zone}...")
    operation = instance_client.delete(
        project=project_id, zone=zone, instance=machine_name
    )
    wait_for_extended_operation(operation, "instance deletion")
    print(f"Instance {machine_name} deleted.")
    return

def getInstanceExternalInternalIpByName(name):
    client = compute_v1.InstancesClient()    
    instanceConfig = client.get(project=PROJECTID,zone=ZONE,instance=name)
    return [instanceConfig.network_interfaces[0].access_configs[0].nat_i_p, instanceConfig.network_interfaces[0].network_i_p]

def setupMachineByhostIP(hostIP: str):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"hostIP is {hostIP} username is {USER} and ssh file name is {SSHFilePath}")
    ssh.connect(hostname= hostIP,username=USER,key_filename=SSHFilePath)
    return ssh
    
def runCommandsOnAMachineOverSSH(ssh,commands):
    for command in commands:
        print(f"Excecuting command {command}")
        stderr = ''
        try:
            stdin, stdout, stderr = ssh.exec_command(command)
            while True:
                line = stdout.readline()
                if not line:
                    break
                print(line, end="")
            print("Command executed successfully is",command)
        except Exception as e:
            print("An error occurred while running the command",command, stderr)       
            


