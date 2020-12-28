# Imports
import os
import pandas as pd

from config import settings

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#%%
#Script parameters
wyscout_dataset = 'soccerlogs_ds.csv'
wyscout_dataset_file = os.path.join(settings.data_folder_path, wyscout_dataset)
df = pd.read_csv(wyscout_dataset_file)
df.describe(include='all').T.to_csv(os.path.join(settings.data_folder_path, 'soccerlogs_summary.csv'))





