# Imports
import os
import pandas as pd

from config import settings

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#%%
#Script parameters
opta_combined_file = 'opta_combined_ds.csv'

#%%
# Merge opta files to create single Female dataset
opta_data_path = os.path.join("./data", 'opta_ds')
ds_file_names = [os.path.join(opta_data_path, file) for file in os.listdir(opta_data_path)]
combined_data = pd.concat([pd.read_csv(f) for f in ds_file_names], sort=False)
print(combined_data.columns)
combined_data.to_csv(os.path.join("./data", opta_combined_file), index=False)

#%%
# Read combined Opta dataset
opta_dataset_file = os.path.join("./data", opta_combined_file)
df = pd.read_csv(opta_dataset_file)




