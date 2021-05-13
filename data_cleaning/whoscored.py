import os

import pandas as pd

from config import settings

df = pd.read_csv("./data/soccerlogs_ds_with_name.csv", index_col=["matchId", "playerId"])

final_id = 1694440
semi_portugal = 1694438
semi_france = 1694439

#%% Data attributes that can be validated through WhoScored scrapping.

df.loc[final_id, 70410][[
    "Shots",
    "Shots_on_target",
    "Aerial_won",
    "Fouls",
    "Interceptions",
    "Clearances",
    "Passes",
    "Successful_passes",
    "Crosses",
    "Successful_crosses",
    "Yellow_card",
    "Goals",
]]
