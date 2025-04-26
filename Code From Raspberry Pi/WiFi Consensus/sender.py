import socket
import json
import threading 
import time
import os
import subprocess
import csv
import pandas as pd
import matplotlib.pyplot as plt

#The sender.py code must be run befor the coordinator.py code. Coordinator.py is responsible to start the consensus in the node and collect back the csv from the nodes and plot them.
#Sender.py is responsible to send every node details, files to every node and run them.


#The below dictionary contains ip address, port number for ssh communication and user name
#Make sure to only use port number as 22 as its the port allotted by default for ssh and scp communication
#If any nodes are added their ip and ports and username are to be added to the dictionary in the same format with the correct node ids

ns = {
    1: {"ip": "192.168.191.142", "port": 22, "user": "rasp1"},
    2: {"ip": "192.168.191.32", "port": 22, "user": "rasp6"},
    3: {"ip": "192.168.191.1", "port": 22, "user": "rasp7"},
    4: {"ip": "192.168.191.111", "port": 22, "user": "rasp8"}
}

#The bottom two lines are to be filled with the ip address and port number of coordinator for udp communication. This is a one time step
local_ip = "169.254.253.114"
local_port = 12345

def transfer_and_run(node_id, node_info):
    #These node info comes from the ns file through the loop in the main fuction
    ip = node_info["ip"]
    port = node_info["port"]
    user = node_info["user"]
    node_script = f"n{node_id}.py"
    
    # Transfer node script
    scp_command = f"scp -P {port} {node_script} {user}@{ip}:/home/{user}/Desktop/"
    print(f"Transferring {node_script} to {user}@{ip}...")
    os.system(scp_command)

    # Run node script
    ssh_command = f"ssh -p {port} {user}@{ip} 'python3 /home/{user}/Desktop/{node_script}'"
    print(f"Running {node_script} on {user}@{ip}...")
    subprocess.run(ssh_command, shell=True)


#Threading is done in the below lines to send data simultaneously to the nodes and run them
ts = []
for node_id, node_info in ns.items():
    thread = threading.Thread(target=transfer_and_run, args=(node_id, node_info))
    ts.append(thread)
    thread.start()

for thread in ts:
    thread.join()