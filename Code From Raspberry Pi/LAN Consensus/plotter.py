import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

nd = 4
# === Step 1: Read the CSV file ===
file_path = f'x_{nd}.csv'  # Update this path if needed
df = pd.read_csv(file_path, header=None)

# === Step 2: Extract x-values and string-formatted arrays ===
x_values = df[0].astype(float)
y_strings = df[1]

# === Step 3: Convert string representations to lists of floats ===
def parse_array_string(s):
    # Removes brackets and splits by whitespace
    numbers = s.replace('[', '').replace(']', '').split()
    return [float(n) for n in numbers]

parsed_values = y_strings.apply(parse_array_string)

# === Step 4: Convert to 2D numpy array ===
data_array = np.array(parsed_values.to_list())  # shape: (observations, 4)
data_array_t = data_array.T  # shape: (4, observations) → for 4 curves

# === Step 5: Plotting ===
# plt.figure(figsize=(10, 6))
for i, variable_data in enumerate(data_array_t):
    plt.plot(x_values, variable_data, label=f'x{i+1}')

plt.title(f"Evolution of solution variable of node{nd}")
plt.xlabel('Iterations')
plt.ylabel('Values')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()