import csv
import matplotlib.pyplot as plt

num_nodes = 4
gaps = 100
iteration_number = 100000
node_id = 1

def data_extractor(fl,d):
    #This function is responsible to take all the data from the csv files received from the nodes and collect the data in them and store them in the data list for plotting
    data = []
    with open(f"x_{fl}.csv", 'r') as file:
        reader = csv.reader(file)
        for row in reader: 
            data.append(float(row[1]))
    d.append(data)
    
d_list = []
time = []

# for i in range(0,num_nodes):
data_extractor(node_id,d_list)

for i in range(1,iteration_number+1,gaps):
    time.append(i)
    
for i in range(0,num_nodes):
    plt.plot(time,d_list[i], label=f'x{i+1}(t)')
plt.xlabel('Time')
plt.ylabel('State')
plt.title('State Evolution of Nodes')
plt.legend()
plt.grid(True)
plt.show()