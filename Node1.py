import socket
import pandas as pd
import time
import json
import matplotlib.pyplot as plt
import csv
import numpy as np

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
beta = 0.01
gamma = 10
delta = 0.01
A = []
b = []
x = []
y = []
z = []
gaps = 100

neighbor_prev_states=[]   #This stores the value of previous states in this list
sleep_time = 1       #This is global sleeptime variable
local_ip = "127.0.0.1" #Local nodes ip address (Node where the consensus runs)
port_num = 12345    #Local nodes port number

def write_state(node_id, x,y,ld):
    #This function is responsible to write the state variable in n__{node_id}.txt
    data = {
        "x": x,
        "y": y,
        "ld": ld
    }
    
    file_path = f"State_{node_id}.json" 
    
    with open(file_path, 'w') as file:
        json.dump(data, file)
    '''
    with open(f'n_{node_id}.txt', 'w') as file:
        file.write(str(state))
    '''

def send_state_to_neighbors(ip, port, node_id, x,y,ld):
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
        
        
        message = {
            "node_id": node_id,
            "x": x_val,
            "y": y_val,
            "ld": ld_val
        }
        # Serialize the dictionary to a JSON string
        json_message = json.dumps(message)
        # Send the JSON string as bytes
        s.sendto(json_message.encode('utf-8'), (ip, port))

def listen_to_neighbors(s,x_neighbor_states,y_neighbor_states,ld_neighbor_states,received_from):
    #This function is called when we need to listen to neighbor to get their states.
    s.settimeout(sleep_time*0.9)
    #Time out is set at 80 percent of sleeptime so that the listening process stops and the node can proceed on using previous state values for computation
    
    try:
        # Receive data
        data, _ = s.recvfrom(1024)
        if data:
            # Deserialize the JSON string back into a dictionary
            message = json.loads(data.decode('utf-8'))
            # Extract node_id and state from the message
            neighbor_id = int(message["node_id"])
            x_state = np.array((message["x"]))
            y_state = np.array((message["y"]))
            ld_state = np.array((message["ld"]))
            # Update the neighbor states dictionary
            x_neighbor_states[neighbor_id] = x_state
            y_neighbor_states[neighbor_id] = y_state
            ld_neighbor_states[neighbor_id] = ld_state 
            received_from.append(neighbor_id)
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


def run_consensus(s,alpha,iter,xt):
    global ii
    # global q
    global neighbor_prev_states,sleep_time
    global x,y,ld
    #neighbor_states = [-1]*(num_nodes+1)      #Neighbor states are made -1 initially for us to identify whether data has been recieved from a node
    x_neighbor_states = [-1]*(num_nodes+1)
    y_neighbor_states = [-1]*(num_nodes+1)
    ld_neighbor_states = [-1]*(num_nodes+1)
    received_from = []
    #validity = [1]*(num_nodes+1)              #Validity gives the authenticity of data like neighbor states
    # with open(state_file, 'r') as f:
    #     state = float(f.read().strip())
    # fp = f"D_{node_id}.json"
    # fdp = f"State_{node_id}.json"
    # A = []
    # b = []
    # x = []
    # y = []
    # ld = []
    # with open(fp, 'r') as file:
    #     data = json.load(file)
    #     A = np.array(data.get("A"))
    #     b = np.array(data.get("b"))
    
    # with open(fdp, 'r') as file:
    #     data = json.load(file)
    #     x = data.get("x")
    #     y = data.get("y")
    #     ld = data.get("ld")
        
    # p = len(A)
    # q = len(A[0])
    
    for _ in range(iter):
        # Send state to all neighbors
        for neighbor in neighbors[node_id]:
            ip = nodes[neighbor-1][0]
            port = nodes[neighbor-1][1]
            send_state_to_neighbors(ip, port, node_id, x, y, ld)

        # This loop listens to neighbors
        for neighbor in neighbors[node_id]:
            listen_to_neighbors(s,x_neighbor_states,y_neighbor_states,ld_neighbor_states,received_from)
        #neighbor_prev_states.append(neighbor_states)   #All these values are appended to previous states list for later use
        
        '''
        for neighbor in neighbors[node_id]:
            #We check whether the data has arrived or not in this loop and then make use of previous state if the data from neighbor has not been arrived
            if(neighbor_states[neighbor]==-1 and xt!=1):
                if(neighbor_prev_states[-2][neighbor]!=-1):
                    #print("Prev state used: " ,neighbor_prev_states[-2][neighbor]," of neighbor:",neighbor)
                    #Use the above commented statement to identify whether there were any use of prev states
                    neighbor_states[neighbor] = neighbor_prev_states[-2][neighbor]
                else:
                    validity[neighbor]=0
        #state_update = sum( neighbor_states[neighbor] - state for neighbor in neighbors[node_id])
        '''
        
        x_update = np.array([[0]*q])
        y_update = np.array([[0]*p])
        ld_update = np.array([[0]*q])
        
        x_update = x_update.T
        y_update = y_update.T
        ld_update =  ld_update.T
        
        #The algorithm part of the distributed consensus goes here.
        #--------------------------------------------------------
        #The below loop is used for average consensus
        # x = np.array(x)
        # y = np.array(y)
        # ld = np.array(ld)
        
        
        x_update = x_update.astype(np.float64)
        y_update = y_update.astype(np.float64)
        ld_update = ld_update.astype(np.float64)
        
        res = np.dot(A,np.dot(A.T,y))
        result = np.dot(A.T,y)
        
        # print(A.T,y)
        # print(x_update)
        # print(result)
        
        result = result.astype(np.float64)
        res = res.astype(np.float64)
    
        
        # for neighbor in neighbors[node_id]:
        #     #x_update += (ld_neighbor_states[neighbor]-ld)
        #     # print(neighbor,"--->")
        #     # print(x_update)
        #     # print(ld)
        #     # print(ld_neighbor_states[neighbor])
        #     x_update += (alpha)*(ld_neighbor_states[neighbor]-ld)
        #     y_update += (gamma*(y_neighbor_states[neighbor]-y))
        #     # y_update += np.dot(A,ld_neighbor_states[neighbor]-ld)
        #     y_update += (alpha)*np.dot(A,ld_neighbor_states[neighbor]-ld)
        #     #print(neighbor,"--",(alpha)*(x_neighbor_states[neighbor]-x),(gamma*(y_neighbor_states[neighbor]-y)),(delta)*np.dot(A,x_neighbor_states[neighbor]-x))
        #     ld_update += (x-x_neighbor_states[neighbor])
            
        #print(x,y,ld)
        
        x_update += ((-2)*num_nodes*beta*(result))
        y_update += -1*(2*num_nodes*beta*(res))
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
        
        for neighbor in received_from:
            #x_update += (ld_neighbor_states[neighbor]-ld)
            # print(neighbor,"--->")
            # print(x_update)
            # print(ld)
            # print(ld_neighbor_states[neighbor]
            x_update += (-1*alpha*(x-x_neighbor_states[neighbor]) - (ld-ld_neighbor_states[neighbor]))
            y_update += (-1*(y-y_neighbor_states[neighbor]) - alpha*(np.dot(A,x-x_neighbor_states[neighbor])) - np.dot(A,ld-ld_neighbor_states[neighbor]))
            ld_update += (x-x_neighbor_states[neighbor])
            #print(neighbor,":",x_neighbor_states[neighbor])
            # v1 += -1*(y-y_neighbor_states[neighbor])
            # v2 += - alpha*(np.dot(A,x-x_neighbor_states[neighbor]))
            # v3 += - np.dot(A,ld-ld_neighbor_states[neighbor])
        # print("Updated x and y:")
        # print(x)
        # print("x up",x_update)
        # print("y up",y_update)
        # print("ld up",ld_update)
        # print("Gap")
        # print("v1:",v1)
        # print("v2:",v2)
        # print("v3:",v3)
        
        # print("----")
        # print(x_update)
        
        x += delta * x_update
        y += delta * y_update
        ld += delta * ld_update
        
        #--------------------------------------------------------
        
        
        # print(f"Node {node_id} Iteration {x}: State = {state}")
        # print_time_taken(f"Iteration {x}:",u_st)
        # Sleep to synchronize with other nodes
        sle = max(0,sleep_time - (time.time()-u_st-sleep_time*(xt-1)))     #Sleep time is calculated based on total sleep time per iteration -  the time lapsed in listening, sending, computation time
        # print("Lapse time:",(sleep_time-sle))
        # print("sleep_time:",sle)
        time.sleep(sle)     #This makes our program sleep for the required time
        
        # write_state(node_id,x,y,ld)   #Writes the new computed state in the text file
        

        if(ii%gaps==0):
            x_val_list.append(x.tolist())
            y_val_list.append(y.tolist())
            ld_val_list.append(ld.tolist())
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
    clear_csv(f"x_{node_id}.csv")
    clear_csv(f"y_{node_id}.csv")
    clear_csv(f"ld_{node_id}.csv")
    
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((local_ip, port_num))
        #Our program waits for data to arrive from the coordinator node.
        #The below while loop breaks when the initiation message reaches this node and the actual computation starts after that
        while True:
            data, _ = s.recvfrom(1024)
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
                    x = x.T
                    y = y.T
                    ld = ld.T
                    x_val_list.append(x)
                    y_val_list.append(y)
                    ld_val_list.append(ld)
                    y = -1*b
                    
                    x = x.astype(np.float64)
                    y = y.astype(np.float64)
                    ld = ld.astype(np.float64)
                    A = A.astype(np.float64)
                    b = b.astype(np.float64)
                    
                    time.sleep(2)
                    #Intial sleep time of 2 seconds ensures that all the program have optimal time to do the preprocessing and also helps in synchronisation
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
                    
                    for i in range(q):
                        plt.plot(time_list, x_array[i], label=f'v{i+1}(t)')
                    
                    
                    plt.xlabel('Iterations')
                    plt.ylabel('States')
                    plt.title(f'States Evolution of Node {node_id}')
                    plt.legend()
                    plt.grid(True)
                    plt.show()
                    break