import os
import pandas as pd

from config import settings

soccerlogs_male_data_df = pd.read_csv(os.path.join(settings.data_folder_path, settings.wyscout_dataset))
opta_female_df = pd.read_csv(os.path.join(settings.data_folder_path, settings.opta_combined_file))
print(opta_female_df.columns)

soccerlogs_discarded_vars = ['matchId', 'teamId', 'playerId']
soccerlogs_error_vars = ['Goals_from_set_play']
soccerlogs_specific_vars = ["Own_goals"]

opta_discarded_vars = ['_team_id', '_player_id']
opta_error_vars = [
    'Clearance_lost',
    'Headed_clearance_lost',
    'Penalty_conceeded',
    'Turnovers',
    'Ball_recoveries',
]
opta_specific_vars = [
    'Tackles_won_with_possession',
    'Tackles_won_without_possession',
    'Tackle_lost_Challenge',
    'Outfielder_saves',
    'Cross_claim_(gk)',
    'Headed_clearance_won',
    'Goals_from_open_play',
    'Goals_from_set_play',
    'Successful_take_on_dribble',
    'Unsuccessful_take_on_dribble',
    'Pass_direction',
    "Own_goals"
]

soccerlogs_male_data_df.drop(columns=soccerlogs_discarded_vars + soccerlogs_error_vars + soccerlogs_specific_vars,
                             inplace=True)
opta_female_df.drop(columns=opta_discarded_vars + opta_error_vars + opta_specific_vars, inplace=True)

# %% Merging
opta_female_df.rename(columns={'_period_id': 'matchPeriod', 'Fouls': 'Fouls_received', 'Clearance_won': 'Clearances'}, inplace=True)
soccerlogs_male_data_df.rename(columns={'Fouls': 'Fouls_received'}, inplace=True)
opta_female_df['matchPeriod'] = opta_female_df['matchPeriod'].replace(1, '1H')
opta_female_df['matchPeriod'] = opta_female_df['matchPeriod'].replace(2, '2H')
opta_female_df['matchPeriod'] = opta_female_df['matchPeriod'].replace(3, 'E1')
opta_female_df['matchPeriod'] = opta_female_df['matchPeriod'].replace(4, 'E2')
opta_female_df['matchPeriod'] = opta_female_df['matchPeriod'].replace(5, 'P')

#%% Player type editing
opta_female_df['PlayerType'] = opta_female_df['PlayerType'].replace(['Striker'], 'Forward')

#%% Combining datasets
soccerlogs_male_data_df['gender'] = '1'
opta_female_df['gender'] = '0'

combined_df = pd.concat([soccerlogs_male_data_df, opta_female_df], sort=False)
combined_df = combined_df[combined_df['Total_touches'] != 0].drop(['Total_touches'], axis=1)
combined_df['gender'].value_counts()

#%% SAVE
combined_df.to_csv(os.path.join(settings.data_folder_path, 'ds.csv'), index=False)