# %% External import statements
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

# %% Internal import statements
from config import pyspark_config

spark = SparkSession.builder.config(conf=pyspark_config.SPARK_CONF).getOrCreate()
#%%
with open(os.path.join('..', 'data', 'soccerlogs_input', 'players.json'), 'r') as f:
    players_list = json.load(f)
players_dict = {}
for player in players_list:
    players_dict[player['wyId']] = player['role']['name']

#%%
with open(os.path.join('..', 'data', 'soccerlogs_input', 'matches', 'matches_European_Championship.json'), 'r') as f:
    matches_list = json.load(f)
matches_dict = {}
for match in matches_list:
    for team, team_data in match['teamsData'].items():
        matches_dict.setdefault(match['wyId'], {})[team] = [sub['playerIn'] for sub in team_data['formation']['substitutions']]


# %%
filePath = os.path.join('data', 'soccerlogs_input', 'events', 'events_European_Championship.json')

df_match = spark.read.json(filePath)
df_match.printSchema()

# %%
df_events = df_match.select(
    'matchId', 'eventId', 'matchPeriod', 'teamId', 'playerId', 'eventSec', 'positions', 'subEventId',
    F.explode('tags').alias('tags_exploded')
).select(
    '*', F.col('tags_exploded.id').alias('tags')
).groupBy(
    'matchId', 'eventId', 'matchPeriod', 'teamId', 'playerId', 'eventSec', 'positions', 'subEventId'
).agg(
    F.collect_list('tags').alias('tags')
)
# %%

query_dictionary = {
    "Passes": {
        "event_type": [80, 81, 82, 83, 84, 85, 86],
    },
    "Successful_passes": {
        "event_type": [80, 81, 82, 83, 84, 85, 86],
        "qualifiers": [1801]
    },
    "Unsuccessful_passes": {
        "event_type": [80, 81, 82, 83, 84, 85, 86],
        "qualifiers": [1802]
    },
    "Crosses": {
        "event_type": [80],
    },
    "Successful_crosses": {
        "event_type": [80],
        "qualifiers": [1801],
    },
    "Unsuccessful_crosses": {
        "event_type": [80],
        "qualifiers": [1801]
    },
    "Long_passes": {
        "event_type": [83]
    },
    "Long_passes_won": {
        "event_type": [83],
        "qualifiers": [1801]
    },
    "Long_passes_lost": {
        "event_type": [83],
        "qualifiers": [1802]
    },
    "Aerial": {
        "event_type": [10]
    },
    "Aerial_won": {
        "event_type": [10],
        "qualifiers": [1801]
    },
    "Aerial_lost": {
        "event_type": [10],
        "qualifiers": [1802]
    },
    "Ground_duels": {
        "event_type": [11, 12, 13]
    },
    "Ground_duels_won": {
        "event_type": [11, 12, 13],
        "qualifiers": [703]
    },
    "Ground_duels_lost": {
        "event_type": [11, 12, 13],
        "qualifiers": [701]
    },
    "Free_kicks": {
        "event_type": [30, 31, 32, 33, 34, 35, 36]
    },
    "Fouls": {
        "event_type": [20, 21, 22, 23, 24, 25, 26, 27]
    },
    "Fouls_conceded": {
        "event_type": [20, 21, 22, 23, 24, 25, 26, 27]
    },
    "Corners": {
        "event_type": [30],
    },
    "Corners_successful": {
        "event_type": [30],
        "qualifiers": [1801],
    },
    "Corners_unsuccessful": {
        "event_type": [30],
        "qualifiers": [1802]
    },
    "Interceptions": {
        "qualifiers": [1401]
    },
    "Saves": {
        "event_type": [90, 91],
        "qualifiers": [1801]
    },
    "Clearance_won": {
        "event_type": [71],
        "qualifiers": [1801]
    },
    "Clearance_lost": {
        "event_type": [71],
        "qualifiers": [1802]
    },
    "Shots": {
        "event_type": [100]
    },
    "Shots_on_target": {
        "event_type": [100],
        "qualifiers": [1801],
    },
    "Shots_off_target": {
        "event_type": [100],
        "qualifiers": [1802],
    },
    "Goals": {
        "event_type": [100],
        "qualifiers": [101]
    },
    "Goals_from_set_play": {
        "event_type": [30, 31, 32, 34, 36],
        "qualifiers": [101]
    },
    "Goals_from_penalty": {
        "event_type": [35],
        "qualifiers": [101]
    },
    "Own_goals": {
        "qualifiers": [102]
    },
    "Yellow_card": {
        "qualifiers": [1702, 1703]
    },
    "Red_card": {
        "qualifiers": [1701]
    },
    "Total_touches": {
        "event_type": [71, 72, 80, 81, 82, 83, 84, 85, 86]
    },
}

auto_counters = df_events.groupBy(
    "matchId", "matchPeriod", "teamId", "playerId",
).agg(
    *[
        F.count(
            F.when(
                (F.col("subEventId").isin(metadata.get('event_type'))) &
                (F.size(F.array_intersect(F.col('tags'),
                                          F.array([F.lit(x) for x in metadata.get("qualifiers")]))) == F.size(
                    F.array([F.lit(x) for x in metadata.get("qualifiers")]))),
                1
            )).alias(column_name) if ('qualifiers' in metadata) and ('event_type' in metadata) else
        F.count(
            F.when(
                (F.col("subEventId").isin(metadata.get('event_type'))),
                1
            )).alias(column_name) if ('event_type' in metadata) else
        F.count(
            F.when(
                (F.size(F.array_intersect(F.col('tags'),
                                          F.array([F.lit(x) for x in metadata.get("qualifiers")]))) == F.size(
                    F.array([F.lit(x) for x in metadata.get("qualifiers")]))),
                1
            )).alias(column_name)
        for column_name, metadata in query_dictionary.items()
    ]
)

#%%
printing_df = auto_counters.toPandas()
printing_df['PlayerType'] = printing_df.apply(
    lambda x: players_dict[x['playerId']] if x['playerId'] not in matches_dict[x['matchId']][str(x['teamId'])] else f"{players_dict[x['playerId']]}Substitute",
    axis=1
)

printing_df.to_csv("../data/soccerlogs_ds.csv", index=False)
print("File saved")