import xarray as xr
import pandas as pd

# Step 1: Load the .nc file
ds = xr.open_dataset('D:\MyWorkspace\Data Sets\RF25_ind2024_rfp25.nc')  # Replace with your file path

# Step 2: Explore the dataset (optional)
print(ds)  # To see variables, dimensions, etc.

# Step 3: Convert to pandas DataFrame
# Example: Select a variable (e.g., 'temperature') and flatten it
df = ds.to_dataframe().reset_index()  # Convert all variables; you can filter later

# Optional: drop NA values
df = df.dropna()

# Step 4: Save to Excel
#df.to_excel('output.xlsx', index=False)
df.to_csv("output.csv", index=False)