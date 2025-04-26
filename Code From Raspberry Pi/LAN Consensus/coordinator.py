import socket
import json
import threading 
import time
import os
import subprocess
import csv
import pandas as pd
import matplotlib.pyplot as plt


A = [
        [[5, 0, 2, 3], [5, 2, 2, 6], [3, 4, 5, 8], [0, 5, 4, 2], [8, 2, 2, 0]],
        [[6, 1, 6, 4], [4, 0, 4, 2], [1, 0, 3, 6], [0, 0, 0, 1], [4, 1, 2, 3]],
        [[2, 1, 0, 3], [6, 2, 3, 5], [1, 4, 6, 0], [2, 3, 2, 2], [0, 3, 1, 0]],
        [[-6, 2, 0, -5], [-8, -1, -2, -5], [0, -4, -6, -6], [1, -2, -1, -3], [-4, 0, -3, 2]]
    ]

b = [
        [[61], [51], [1], [55], [1]],
        [[112], [11], [2], [80], [1]],
        [[57], [38], [100], [58], [53]],
        [[-117], [-4], [6], [-107], [12]]
    ]

ns = {
    1: {"ip": "169.254.71.146", "port": 22, "user": "rasp1"},
    2: {"ip": "169.254.8.59", "port": 22, "user": "rasp6"},
    3: {"ip": "169.254.19.87", "port": 22, "user": "rasp7"},
    4: {"ip": "169.254.228.213", "port": 22, "user": "rasp8"}
}

#The bottom two lines are to be filled with the ip address and port number of coordinator for udp communication. This is a one time step
local_ip = "169.254.253.114"
local_port = 12345

def print_time_taken(description, start_time):
    end_time = time.time()
    print(f"{description} took {end_time - start_time:.6f} seconds")
    #This function is used to print the time taken from start of each step when the start_time inputted in the function is the time recorded at the start
    #Currently in this code all the print_time_functions are removed. If necessary the user can use this to get an idea of execution time

def send_init_message(ip, port, node_id,iteration_number,neighbors,alpha,iter,nodes,beta,gamma,delta,gaps,initiation,indegree):
    #This function is responsible to send initialisation message of all the nodes when the coordinator iteration starts.
    #Along with the initialisation message things like iteration number, alpha, sleeptime,etc.. are sent.
    message = {
        "init": "INIT",
        "inum": iteration_number,
        "alpha": alpha,
        "iter": iter,
        "neighbors": neighbors,
        "nodes": nodes,
        "num_nodes": num_nodes,
        "sleep_time" : sleep_time,
        "beta": beta,
        "gamma": gamma,
        "delta": delta,
        "A": A[node_id-1],
        "b": b[node_id-1],
        "gaps": gaps,
        "node_id": node_id,
        "strt": initiation,
        "indegree": indegree
    }
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.sendto(json.dumps(message).encode('utf-8'), (ip, port))   #This line sends all the above data in message to the ip of the node and its port.
        #ip and port in the above statement denotes the ip and port in which the destination nodes listens

def send_messages_to_node(ip, port, node_id, neighbors, nodes,iteration_number,alpha,iter,beta,gamma,delta,gaps,initiation,indegree):
    send_init_message(ip, port, node_id, iteration_number,neighbors,alpha,iter,nodes,beta,gamma,delta,gaps,initiation,indegree)
    
if __name__ == "__main__":
    nodes = [("169.254.71.146",12345,1), ("169.254.8.59",12345,2), ("169.254.19.87",12345,3), ("169.254.228.213",12345,4)]   #Node information is stored in this list
    #The nodes list contains every nodes ip address along with the port which the node listens. The above line is to changed while changing the order and number of nodes.
    #The tuple at index 0 represents node 1 and index 1 represents node 2 and so on.
    #Its important to make sure the node numbers match with their ip addresses in both coordinator and node.py file
    iteration_number = 100000  #Its upto the user to decide the number of iterations. More the iterations accurate are the results
    sleep_time = 0.002      #Sleep time has some constraints
    gaps = 100
    initiation = 5
    # For LAN connection sleep time can not be less than 0.0033 seconds for good results
    # For WiFi connection sleep time can not go lesser than 0.1 seconds for accurate results
    neighbors = [
        [],
        [2,3,4],
        [1],
        [2],
        [2],
    ]
    neighbors = [
        [],
        [2],
        [1,3,4],
        [1],
        [1],
    ]
    siz = len(neighbors)
    indegree = [0]*siz
    for i in range(1,siz):
        for j in neighbors[i]:
            indegree[j]+=1
    print(indegree)
    # Neighbor list is supposed to be modified to change the edges of the graph which is then responsible for change in communication
    num_nodes = 4     #Change this when there is a change in number of nodes
    alpha = 25 
    beta = 5e-2
    gamma = 5e-3
    delta = 1e-3   
    iter = 1
    start_time = time.time()  #This measures the time taken for each step for our use. These can be removed to make the code run faster
    threads = []
    counter = 0
    #This portion of the code is responsible to perform threading so that all the messages sent to the nodes are sent simultaneously.
    #The threads list initially stores all the threads that are to be started. Its then initiated in the loop below for messages to be sent simultaneously
    for node in nodes:
        thread = threading.Thread(target=send_messages_to_node, args=(node[0], node[1], node[2], neighbors, nodes,iteration_number,alpha,iter,beta,gamma,delta,gaps,initiation,indegree))
        threads.append(thread)
        counter += 1
    time.sleep(1)
    
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Initialization messages sent to all nodes.")
    
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((local_ip,local_port))
        cnt = 0
        while(cnt<num_nodes):
            s.settimeout(sleep_time)
            try:
                data, _ = s.recvfrom(65535)
                if data:
                    node_id, msg = data.decode('utf-8').split(',')
                    if(msg=="done"):
                        cnt+=1
                    
            except socket.timeout:
                pass
    print("Completion message received from all nodes.")