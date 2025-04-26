import socket
import pandas as pd
import time
import json
import matplotlib.pyplot as plt
import csv
import numpy as np
import os
import paramiko

def send_file(file,ip,user,path):
    #This function uses paramiko module the send a file from the nodes back to the coordinator.
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip,username=user,port=22)

        sftp = ssh.open_sftp()
        remote_path = os.path.join(path,os.path.basename(file))
        sftp.put(file,remote_path)
        sftp.close()
        ssh.close()
        print(f"File {file} successfully sent.")
    except Exception as e:
        print(f"File Transfer Failed due to {e}")

#This file is for node 1. Other nodes has the same code except for the following variables. All the values must match with the values in coordinator file.
#          node_id in line 20 to the node id given in coordinator
#          local_ip in line 25 to the ip of the device that was noted
#          port_num in line 26 to the port number used for communication

# When switching from LAN to Internet as a mode of communication the IPs of the coordinator along with the ips in the node is supposed to be changed. There are no other changes other than that
# The node_id in each node is the 1-based index of nodes list in the coordinator file that contains the ip of the particular node.

#All the variables below are global. These data will be changed once the intiation message from the coordinator arrives.
#The data below are not data that will be used for iteration

u_st = 0            #u_st stores the start time of the process
neighbors=[]        #This list stores the neighbor data
nodes = []          #This list stores IP Address and port numbers 
node_id = 1
num_nodes = 5
n = num_nodes
x_val_list = []         #This list stores the node values at each iteration of its own node
y_val_list = []         #This list stores the node values at each iteration of its own node
ld_val_list = []         #This list stores the node values at each iteration of its own node
v_val_list = []
w_val_list = []
beta = 0.01
gamma = 10
delta = 0.01
A = []
b = []
x = []
y = []
z = []
L = []
L_dash = []
gaps = 1000
strt = 10
indegree = []
p = 0
q = 0
found_L = 0
x_vals = np.array([])
y_vals = np.array([])
ld_vals = np.array([])
v_vals = np.array([])
w_vals = np.array([])

neighbor_prev_states=[]   #This stores the value of previous states in this list
sleep_time = 1       #This is global sleeptime variable
local_ip = "169.254.8.59" #Local nodes ip address (Node where the consensus runs)
port_num = 12345    #Local nodes port number
user_name = "rasp2"               #This is coordinator's user name and its supposed to be entered as a string here
coor_ip = "169.254.253.114"      #This is coordinator's ip address and its supposed to be entered as a string here
coor_port = 12345

def send_state_to_neighbors(ip, port, node_id, x,y,ld,v,w):
    #This function sends the state of this node to the neighbor nodes whose ip address and port are the variable ip and port using UDP Protocol
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        '''
        message = f"{node_id},{x},{y},{ld}"
        #Data is sent and received as a string with node_id and state separated by comma
        s.sendto(message.encode('utf-8'), (ip, port))
        '''
        x_val = x.tolist()
        y_val = y.tolist()
        ld_val = ld.tolist()
        v_val = v.tolist()
        w_val = w.tolist()
        
        
        message = {
            "node_id": node_id,
            "x": x_val,
            "y": y_val,
            "ld": ld_val,
            "v": v_val,
            "w": w_val
        }
        # Serialize the dictionary to a JSON string
        json_message = json.dumps(message)
        # Send the JSON string as bytes
        s.sendto(json_message.encode('utf-8'), (ip, port))

def listen_to_neighbors(s,received_from):
    global x_vals,y_vals,ld_vals,v_vals,w_vals
    #This function is called when we need to listen to neighbor to get their states.
    s.settimeout(sleep_time*0.9)
    #Time out is set at 80 percent of sleeptime so that the listening process stops and the node can proceed on using previous state values for computation
    
    try:
        # Receive data
        data, _ = s.recvfrom(65535)
        if data:
            # Deserialize the JSON string back into a dictionary
            message = json.loads(data.decode('utf-8'))
            # Extract node_id and state from the message
            neighbor_id = int(message["node_id"])
            x_state = np.array((message["x"]))
            y_state = np.array((message["y"]))
            ld_state = np.array((message["ld"]))
            v_state = np.array((message["v"]))
            w_state = np.array((message["w"]))
            
            
            x_vals[(neighbor_id-1)*q:(neighbor_id-1)*q+q] = x_state
            y_vals[(neighbor_id-1)*p:(neighbor_id-1)*p+p] = y_state
            ld_vals[(neighbor_id-1)*q:(neighbor_id-1)*q+q] = ld_state
            v_vals[(neighbor_id-1)*q:(neighbor_id-1)*q+q] = v_state
            w_vals[(neighbor_id-1)*p:(neighbor_id-1)*p+p] = w_state
            
            # print((node_id-1)*q,(node_id-1)*q+q)
            # print(y_vals)
            
            # Update the neighbor states dictionary
            # x_neighbor_states[neighbor_id] = x_state
            # y_neighbor_states[neighbor_id] = y_state
            # ld_neighbor_states[neighbor_id] = ld_state 
            # v_neighbor_states[neighbor_id] = v_state
            # w_neighbor_states[neighbor_id] = w_state
            received_from[neighbor_id] = 1
            # Optional: Print for debugging
            # print(f"---> Neighbor {neighbor_id} has received state value:")
            # print(x_state)
            # print(y_state)
            # print(ld_state)
    except socket.timeout:
        pass
    
    
    '''
    try:
        data, _ = s.recvfrom(1024)      #Data is received here
        if data:
            neighbor_id, neighbor_state ,= data.decode('utf-8').split(',')    #Data is extracted after decode and splitting.
            neighbor_states[int(neighbor_id)] = float(neighbor_state)        #Global neighbor states list is modified to add new states in it for computation
            #print(f"--->Neighbor {neighbor_id} has state value :",neighbor_state)
            #Above line can be uncommented to see what states are received from each neighbor
            
    except socket.timeout:
        pass
    '''

def log_state(fp,node_id, iteration, state):
    #This function is used to append the current state values at every iteration for storing them and plotting them
    with open(fp, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([iteration, state])


def log_state(fp,node_id, iteration, state):
    #This function is used to append the current state values at every iteration for storing them and plotting them
    with open(fp, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([iteration, state])
        
def run_consensus(s,alpha,iter,xt):
    global ii,found_L,p,q,L,L_dash
    global x_vals,y_vals,ld_vals,v_vals,w_vals
    # global q
    global neighbor_prev_states,sleep_time
    global x,y,ld,v,w
    #neighbor_states = [-1]*(num_nodes+1)      #Neighbor states are made -1 initially for us to identify whether data has been recieved from a node
    # x_neighbor_states = [-1]*(num_nodes+1)
    # y_neighbor_states = [-1]*(num_nodes+1)
    # ld_neighbor_states = [-1]*(num_nodes+1)
    # v_neighbor_states = [-1]*(num_nodes+1)
    # w_neighbor_states = [-1]*(num_nodes+1)
    
    received_from = [0]*(num_nodes+1)
    
    for _ in range(iter):
        # Send state to all neighbors
        for neighbor in neighbors[node_id]:
            ip = nodes[neighbor-1][0]
            port = nodes[neighbor-1][1]
            send_state_to_neighbors(ip, port, node_id, x, y, ld, v, w)

        # This loop listens to neighbors
        for _ in range(indegree[node_id]):
            listen_to_neighbors(s,received_from)
        #neighbor_prev_states.append(neighbor_states)   #All these values are appended to previous states list for later use
        
        x_update = np.array([[0]*q])
        y_update = np.array([[0]*p])
        ld_update = np.array([[0]*q])
        v_update = np.array([[0]*q])
        w_update = np.array([[0]*p])
        
        x_update = x_update.T
        y_update = y_update.T
        ld_update =  ld_update.T
        v_update =  v_update.T
        w_update =  w_update.T
        
        
        #The algorithm part of the distributed consensus goes here.
        #--------------------------------------------------------
        #The below loop is used for average consensus
        # x = np.array(x)
        # y = np.array(y)
        # ld = np.array(ld)
        
        
        x_update = x_update.astype(np.float64)
        y_update = y_update.astype(np.float64)
        ld_update = ld_update.astype(np.float64)
        v_update = v_update.astype(np.float64)
        w_update = w_update.astype(np.float64)
        
        
        
        if(found_L==0):
            L = np.array([])
            L_dash = np.array([])
        
            L = L.astype(np.float64)
            L_dash = L_dash.astype(np.float64)
            
            for its in range(1,num_nodes+1):
                if(its==1):
                    if(its==node_id):
                        L = len(neighbors[its])*np.eye(q)
                        L_dash = len(neighbors[its])*np.eye(p)
                    elif(received_from[its]==0):
                        L = np.zeros((q,q))
                        L_dash = np.zeros((p,p))
                    elif(received_from[its]==1):
                        L = -1*np.eye(q)
                        L_dash = -1*np.eye(p)
                else:
                    if(its==node_id):
                        L = np.concatenate((L,len(neighbors[its])*np.eye(q)),axis=1)
                        L_dash = np.concatenate((L_dash,len(neighbors[its])*np.eye(p)),axis=1)
                    elif(received_from[its]==0):
                        L = np.concatenate((L,np.zeros((q,q))),axis=1)
                        L_dash = np.concatenate((L_dash,np.zeros((p,p))),axis=1)
                    elif(received_from[its]==1):
                        L = np.concatenate((L,-1*np.eye(q)),axis=1)
                        L_dash = np.concatenate((L_dash,-1*np.eye(p)),axis=1)
            found_L = 1
        
        # for its in range(1,num_nodes+1):
        #     if(its==1):
        #         if(its==node_id):
        #             x_vals = x
        #             y_vals = y
        #             ld_vals = ld
        #             v_vals = v
        #             w_vals = w
        #         elif(received_from[its]==0):
        #             x_vals = np.zeros((q,1))
        #             y_vals = np.zeros((p,1))
        #             ld_vals =np.zeros((q,1))
        #             v_vals = np.zeros((q,1))
        #             w_vals = np.zeros((p,1))
        #         elif(received_from[its]==1):
        #             x_vals = x_neighbor_states[its]
        #             y_vals = y_neighbor_states[its]
        #             ld_vals = ld_neighbor_states[its]
        #             v_vals = v_neighbor_states[its]
        #             w_vals = w_neighbor_states[its]
        #     else:
        #         if(its==node_id):
        #             L = np.concatenate((L,len(neighbors[its])*np.eye(q)),axis=1)
        #             L_dash = np.concatenate((L_dash,len(neighbors[its])*np.eye(p)),axis=1)
        #             x_vals = np.concatenate((x_vals,x),axis=0)
        #             y_vals = np.concatenate((y_vals,y),axis=0)
        #             ld_vals = np.concatenate((ld_vals,ld),axis=0)
        #             v_vals = np.concatenate((v_vals,v),axis=0)
        #             w_vals = np.concatenate((w_vals,w),axis=0)
        #         elif(received_from[its]==0):
        #             L = np.concatenate((L,np.zeros((q,q))),axis=1)
        #             L_dash = np.concatenate((L_dash,np.zeros((p,p))),axis=1)
        #             x_vals = np.concatenate((x_vals,np.zeros((q,1))),axis=0)
        #             y_vals = np.concatenate((y_vals,np.zeros((p,1))),axis=0)
        #             ld_vals = np.concatenate((ld_vals,np.zeros((q,1))),axis=0)
        #             v_vals = np.concatenate((v_vals,np.zeros((q,1))),axis=0)
        #             w_vals = np.concatenate((w_vals,np.zeros((p,1))),axis=0)
        #         elif(received_from[its]==1):
        #             L = np.concatenate((L,-1*np.eye(q)),axis=1)
        #             L_dash = np.concatenate((L_dash,-1*np.eye(p)),axis=1)
        #             x_vals = np.concatenate((x_vals,x_neighbor_states[its]),axis=0)
        #             y_vals = np.concatenate((y_vals,y_neighbor_states[its]),axis=0)
        #             ld_vals = np.concatenate((ld_vals,ld_neighbor_states[its]),axis=0)
        #             v_vals = np.concatenate((v_vals,v_neighbor_states[its]),axis=0)
        #             w_vals = np.concatenate((w_vals,w_neighbor_states[its]),axis=0)
        
        x_vals[(node_id-1)*q:(node_id-1)*q+q] = x
        y_vals[(node_id-1)*p:(node_id-1)*p+p] = y
        ld_vals[(node_id-1)*q:(node_id-1)*q+q] = ld
        v_vals[(node_id-1)*q:(node_id-1)*q+q] = v
        w_vals[(node_id-1)*p:(node_id-1)*p+p] = w
        
        
        res = np.dot(A,np.dot(A.T,y))
        result = np.dot(A.T,y)
        
        result = result.astype(np.float64)
        res = res.astype(np.float64)

        V = np.diag(v_vals.flatten())
        W = np.diag(w_vals.flatten())
        
        
        
        x_update += ((-2)*num_nodes*beta*(result) - 1*alpha*np.dot(np.dot(L,V),x_vals) - 1*np.dot(np.dot(L,V),ld_vals))
        y_update += (-1*np.dot(np.dot(L_dash,W),y_vals) -1*(2*num_nodes*beta*(res)) - 1*alpha*np.dot(np.dot(A,L),np.dot(V,x_vals)) - np.dot(np.dot(A,L),np.dot(V,ld_vals)))
        ld_update += (np.dot(np.dot(L,V),x_vals))
        v_update += (-np.dot(L,v_vals))
        w_update += (-np.dot(L_dash,w_vals))
        
        # print("***")
        # print(L)
        # print("--")
        # print((-2)*num_nodes*beta*(result))
        # print("--")
        # print(- 1*alpha*np.dot(np.dot(L,V),x_vals))
        # print("--")
        # print(- 1*np.dot(np.dot(L,V),ld_vals))
        
        '''0000'''
        
        # print(L)
        # print("--")
        # print(V)
        # print("--")
        # print(x_vals)
        
        
        
        
        # x_update += ((-2)*num_nodes*beta*(result))
        # y_update += -1*(2*num_nodes*beta*(res))
        # print("norm y:")
        # print(y_update)

        # v1 = np.array([[0]*p])
        # v2 = np.array([[0]*p])
        # v3 = np.array([[0]*p])
        # v1 = v1.T
        # v2 = v2.T
        # v3 = v3.T
        # v1 =v1.astype(np.float64)
        # v2 = v2.astype(np.float64)
        # v3 = v3.astype(np.float64)
        
        '-----'
        
        # x_update += (-1*alpha*(x)*v[node_id-1] - (ld)*v[node_id-1])
        # y_update += (-1*(y*w[node_id-1]) - alpha*(np.dot(A,v[node_id-1]*x)) - np.dot(A,ld*v[node_id-1]))
        # ld_update += (v[node_id-1]*x)
        # v_update += (-1*(v*indegree[node_id]))
        # w_update += (-1*(w*indegree[node_id]))
        
        

        # for neighbor in received_from:
        #     x_update += (-1*alpha*(-v[neighbor-1]*x_neighbor_states[neighbor]) - (-v[neighbor-1]*ld_neighbor_states[neighbor]))
        #     y_update += (-1*(-w[neighbor-1]*y_neighbor_states[neighbor]) - alpha*(np.dot(A,-v[neighbor-1]*x_neighbor_states[neighbor])) - np.dot(A,-v[neighbor-1]*ld_neighbor_states[neighbor]))
        #     ld_update += (-v[neighbor-1]*x_neighbor_states[neighbor])
        #     v_update += (-1*(-v_neighbor_states[neighbor]))
        #     w_update += (-1*(-w_neighbor_states[neighbor]))
        
        '-----'
        
        # print("x up",x_update)
        # print("y up",y_update)
        # print("ld up",ld_update)
        # print("v up",v_update)
        # print("w up",w_update)
        
        '''000'''
        
        
        x += delta * x_update
        y += delta * y_update
        ld += delta * ld_update
        v += delta * v_update
        w += delta * w_update
        
        
        
        #--------------------------------------------------------
        
        
        sle = max(0,sleep_time - (time.time()-u_st-sleep_time*(xt-1)))     #Sleep time is calculated based on total sleep time per iteration -  the time lapsed in listening, sending, computation time
        
        time.sleep(sle)     #This makes our program sleep for the required time
        
        # write_state(node_id,x,y,ld)   #Writes the new computed state in the text file
        

        if(ii%gaps==0):
            x_val_list.append(x.tolist())
            y_val_list.append(y.tolist())
            ld_val_list.append(ld.tolist())
            v_val_list.append(v.tolist())
            w_val_list.append(w.tolist())
            
            log_state(f"x_{node_id}.csv",node_id, ii, x) #This line adds the values to csv file
            log_state(f"y_{node_id}.csv",node_id, ii, y) #This line adds the values to csv file
            log_state(f"ld_{node_id}.csv",node_id, ii, ld) #This line adds the values to csv file
        
        ii+=1

def clear_csv(file_path):
    #Used to clear the csv file
    with open(file_path, 'w') as file:
        pass

if __name__ == "__main__":
    q = 0
    ii = 1
    #Actual code starts here
    iterations = 50
    alpha = 0.1
    iter = 1
    
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((local_ip, port_num))
        #Our program waits for data to arrive from the coordinator node.
        #The below while loop breaks when the initiation message reaches this node and the actual computation starts after that
        while True:
            data, _ = s.recvfrom(65535)
            if data:
                message = json.loads(data.decode('utf-8'))
                #If message has INIT value the remaining part of the message is then broken down to extract the vales
                if message.get("init") == "INIT":
                    print("INITIATED")
                    #All other values like neighbors, neighbor ips, ports , sleep time are extracted from the message
                    nodes = message["nodes"]
                    neighbors = message["neighbors"]
                    iterations = message["inum"]
                    iter = message["iter"]
                    alpha = message["alpha"]
                    num_nodes = message["num_nodes"]
                    sleep_time = message["sleep_time"]
                    beta = message["beta"]
                    gamma = message["gamma"]
                    delta = message["delta"]
                    A = message["A"]
                    b = message["b"]
                    gaps = message["gaps"]
                    node_id = message["node_id"]
                    strt = message["strt"]
                    indegree = message["indegree"]
                    
                    num_nodes = len(neighbors) - 1
                    
                    #State file and csv file paths are saved in a variable as a string
                    #state_file = f'State_{node_id}.json'
                    #csv_file = f'node{node_id}_state.csv'
                    num_iterations = 50

                    #The current state is read from the file
                    # with open(state_file, 'r') as f:
                    #     state = float(f.read().strip())
                    
                    # with open(state_file, 'r') as file:
                    #     data = json.load(file)
                    #     x = np.array(data.get("x"))
                    #     y = np.array(data.get("y"))
                    #     ld = np.array(data.get("ld"))
                    
                    p = len(A)
                    q = len(A[0])
                    A = np.array(A)
                    b = np.array(b)
                    time_list = []
                    x = np.array([[0]*q])
                    y = np.array([[0]*p])
                    ld = np.array([[0]*q])
                    v = np.array([[strt]*q])
                    w = np.array([[strt]*p])
                    
                    x_vals = np.array([[0]*num_nodes*q])
                    y_vals = np.array([[0]*num_nodes*p])
                    ld_vals = np.array([[0]*num_nodes*q])
                    v_vals = np.array([[0]*num_nodes*q])
                    w_vals = np.array([[0]*num_nodes*p])
                    
                    x_vals = x_vals.T
                    y_vals = y_vals.T
                    ld_vals = ld_vals.T
                    v_vals = v_vals.T
                    w_vals = w_vals.T
                    
                    
                    x = x.T
                    y = y.T
                    ld = ld.T
                    v = v.T
                    w = w.T
                    
                    x_val_list.append(x)
                    y_val_list.append(y)
                    ld_val_list.append(ld)
                    v_val_list.append(v)
                    w_val_list.append(w)
                    y = -1*b
                    
                    x = x.astype(np.float64)
                    y = y.astype(np.float64)
                    ld = ld.astype(np.float64)
                    v = v.astype(np.float64)
                    w = w.astype(np.float64)
                    
                    x_vals = x_vals.astype(np.float64)
                    y_vals = y_vals.astype(np.float64)
                    ld_vals = ld_vals.astype(np.float64)
                    v_vals = v_vals.astype(np.float64)
                    w_vals = w_vals.astype(np.float64)       
                    
                    A = A.astype(np.float64)
                    b = b.astype(np.float64)  
                    
                    clear_csv(f"x_{node_id}.csv")
                    clear_csv(f"y_{node_id}.csv")
                    clear_csv(f"ld_{node_id}.csv")
                    
                    
                    time.sleep(3)
                    #Intial sleep time of 3 seconds ensures that all the program have optimal time to do the preprocessing and also helps in synchronisation
                    u_st = time.time()
                    for i in range(1,iterations+1):
                        #The consensus iterations happen here.
                        run_consensus(s,alpha,iter,i)
                        # print(i)
                        if(i%gaps==0):
                            time_list.append(i) 
                            print(i)
                    
                    #The code below is fully responsible for plotting the graph of our values with iterations
                    x_array = []
                    for i in range(q):
                        xy = [] 
                        for j in range(0,iterations//gaps):
                            xy.append(x_val_list[j][i])
                        x_array.append(xy)
                        
                    #print(beta,gamma,delta)
                    
                    end_t = time.time()
                    
                    print(end_t-u_st,"seconds")
                        
                    
                    # The below three lines are responsible to send done message back to the coordinator for it to conclude the process by plotting the graph
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as soc:
                        message = f"{node_id},done"
                        soc.sendto(message.encode('utf-8'), (coor_ip,coor_port))
                    
                    #The two lines below are responsible to send the .csv file back to the coordinator
                    path = f"/home/{user_name}/Desktop/Single_Pi_Control/Linear_Eq/"
                    csv_x = f"x_{node_id}.csv"
                    send_file(csv_x,coor_ip,user_name,path)
                    
                    csv_y = f"y_{node_id}.csv"
                    send_file(csv_y,coor_ip,user_name,path)
                    csv_ld = f"ld_{node_id}.csv"
                    send_file(csv_ld,coor_ip,user_name,path)


                    break

                    # s.close()
                    
                    
                    #The below lines are for plotting. Since these data are sent back to the coordinator there is no necessity to have these lines in our code.
                    # plt.plot(time_list, val_list , label='x(t)')
                    # plt.xlabel('Time')
                    # plt.ylabel('State')
                    # plt.title(f'State Evolution of Node {node_id}')
                    # plt.legend()
                    # plt.grid(True)
                    # plt.show()
                    # break        

