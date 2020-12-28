# %% External import statements
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# %% Internal import statements
from config import pyspark_config

spark = SparkSession.builder.config(conf=pyspark_config.SPARK_CONF).getOrCreate()
#%%
count_of_events = 0
for folder in os.listdir(os.path.join('/home/project', 'data/opta_input')):
    print(f"Folder: {folder}")
    for file in [f for f in os.listdir(os.path.join('/home/marc/Development/dshs/uefa_project/project', 'data/opta_input', folder)) if '2' not in f and not f.startswith('.')]:
        print(f"File: {file}")
        # %% Arguments
        filePath = os.path.join('/home/marc/Development/dshs/uefa_project/project', 'data/opta_input', folder,
                                "{}".format(file))
        filePath2 = os.path.join('/home/marc/Development/dshs/uefa_project/project', 'data/opta_input', folder,
                                 "{}2.xml".format(file.split('.')[0]))

        # %%
        df_match = spark.read.format(
            'xml'
        ).options(
            rowTag='Games',
            valuetag="tagValue"
        ).option(
            "nullValue", ""
        ).load(filePath)
        print("DF_match loaded")

        # %% Loading players data
        df_players = spark.read.format(
            'xml'
        ).options(
            rowTag='MatchPlayer',
            valuetag="tagValue"
        ).option(
            "nullValue", ""
        ).load(filePath2)

        players_dict = {}
        players = df_players.select('_PlayerRef', '_Position', '_SubPosition').collect()
        for p in players:
            players_dict[p['_PlayerRef']] = "{}{}".format(p['_SubPosition'] or '', p['_Position'])
        print("Players dictionary loaded")

        # %%
        df_events = df_match.select(
            'Game.*'
        ).select(
            F.explode('Event').alias("Events")
        ).select(
            'Events.*'
        )

        count_of_events += df_events.count()

        # %%
        df_events_summary = df_events.select(
            "_event_id", "_period_id", "_team_id", "_player_id", "_type_id", "_outcome",
            F.explode('Q').alias('Qualifiers')
        ).withColumn(
            "pass_direction",
            F.when(
                (F.col("_type_id") == 1) &
                (F.col("Qualifiers._qualifier_id") == 213),
                F.col("Qualifiers._value")
            )
        ).select(
            '*', F.col('Qualifiers._qualifier_id').alias('q_id'), F.col('Qualifiers._value').alias('q_value')
        ).groupBy(
            "_event_id", "_period_id", "_team_id", "_player_id", "_type_id", "_outcome"
        ).agg(
            F.first("pass_direction").alias("pass_direction"),
            F.collect_list(F.col('q_id')).alias('q_ids'),
            F.collect_list(F.col('q_value')).alias('q_values')
        )
        print("Dataframe exploded with qualifiers")

        # %%
        query_dictionary = {
            "Passes": {
                "event_type": [1],
                "not_qualifiers": [2, 5, 6, 107, 123,124],
            },
            "Successful_passes": {
                "event_type": [1],
                "not_qualifiers": [2, 5, 6, 107, 123,124],
                "outcome": 1
            },
            "Unsuccessful_passes": {
                "event_type": [1],
                "not_qualifiers": [2, 5, 6, 107, 123,124],
                "outcome": 0
            },
            "Crosses": {
                "event_type": [1],
                "qualifiers": [2],
                "not_qualifiers": [5, 6],
            },
            "Successful_crosses": {
                "event_type": [1],
                "qualifiers": [2],
                "outcome": 1,
                "not_qualifiers": [5, 6]
            },
            "Unsuccessful_crosses": {
                "event_type": [1],
                "qualifiers": [2],
                "outcome": 0,
                "not_qualifiers": [5, 6]
            },
            "Long_passes": {
                "event_type": [1],
                "qualifiers": [1],
                "not_qualifiers": [2, 5, 6, 107, 123,124],
            },
            "Long_passes_won": {
                "event_type": [1],
                "qualifiers": [1],
                "not_qualifiers": [2, 5, 6, 107, 123,124],
                "outcome": 1
            },
            "Long_passes_lost": {
                "event_type": [1],
                "qualifiers": [1],
                "not_qualifiers": [2, 5, 6, 107, 123,124],
                "outcome": 0
            },
            "Aerial": {
                "event_type": [44]
            },
            "Aerial_won": {
                "event_type": [44],
                "outcome": 1
            },
            "Aerial_lost": {
                "event_type": [44],
                "outcome": 0
            },
            "Ground_duels": {
                "event_type": [3,4,7,45,54]
            },
            "Ground_duels_won": {
                "event_type": [3,4,7,54],
                "outcome": 1
            },
            "Ground_duels_lost": {
                "event_type": [3,4,7,45,50],
                "outcome": 0
            },
            "Free_kicks": {
                "event_type": [1],
                "qualifiers": [5]
            },
            "Fouls": {
                "event_type": [4]
            },
            "Fouls_won": {
                "event_type": [4],
                "outcome": 1
            },
            "Fouls_conceded": {
                "event_type": [4],
                "outcome": 0
            },
            "Corners": {
                "event_type": [1],
                "qualifiers": [6]
            },
            "Corners_successful": {
                "event_type": [1],
                "qualifiers": [6],
                "outcome": 1
            },
            "Corners_unsuccessful": {
                "event_type": [1],
                "qualifiers": [6],
                "outcome": 0
            },
            "Interceptions": {
                "event_type": [8]
            },
            "Tackles_won_with_possession": {
                "event_type": 7,
                "outcome": 1
            },
            "Tackles_won_without_possession": {
                "event_type": 7,
                "outcome": 0
            },
            "Tackle_lost_Challenge": {
                "event_type": 45,
                "outcome": 0
            },
            "Saves": {
                "event_type": 10,
                "outcome": 1
            },
            "Outfielder_saves": {
                "event_type": 10,
                "outcome": 1,
                "qualifiers": [94]
            },
            "Cross_claim_(gk)": {
                "event_type": 11,
                "outcome": 1
            },
            "Clearance_won": {
                "event_type": 12,
                "outcome": 1
            },
            "Clearance_lost": {
                "event_type": 12,
                "outcome": 0
            },
            "Headed_clearance_won": {
                "event_type": 12,
                "outcome": 1,
                "qualifiers": [15]
            },
            "Headed_clearance_lost": {
                "event_type": 12,
                "outcome": 0,
                "qualifiers": [15]
            },
            "Shots": {
                "event_type": [13,14,15,16]
            },
            "Shots_on_target": {
                "event_type": [15,16],
                "not_qualifiers": [82],
            },
            "Shots_off_target": {
                "event_type": [13,14]
            },
            "Goals": {
                "event_type": [16],
                "outcome": 1
            },
            "Goals_from_open_play": {
                "event_type": [16],
                "outcome": 1,
                "qualifiers": [22]
            },
            "Goals_from_set_play": {
                "event_type": [16],
                "outcome": 1,
                "qualifiers": [24]
            },
            "Goals_from_penalty": {
                "event_type": [16],
                "outcome": 1,
                "qualifiers": [9]
            },
            "Own_goals": {
                "event_type": [16],
                "outcome": 1,
                "qualifiers": [28]
            },
            "Successful_take_on_dribble": {
                "event_type": [3],
                "outcome": 1
            },
            "Unsuccessful_take_on_dribble": {
                "event_type": [3],
                "outcome": 0
            },
            "Penalty_conceeded": {
                "event_type": [17],
                "qualifiers": [9],
                "outcome": 0
            },
            "Yellow_card": {
                "event_type": [17],
                "qualifiers": [31,32]
            },
            "Red_card": {
                "event_type": [17],
                "qualifiers": [31,32]
            },
            "Total_touches": {
                "event_type": [1,2,3,7,8,9,10,11,12,13,14,15,16,41,42,50,52,54,61],
                "not_qualifier": [123]
            },
            "Turnovers": {
                "event_type": [9]
            },
            "Ball_recoveries": {
                "event_type": [49]
            }
        }
        auto_counters = df_events_summary.groupBy(
            "_period_id", "_team_id", "_player_id",
        ).agg(
            *[
                F.count(
                    F.when(
                        (F.col("_type_id").isin(metadata.get('event_type'))) &
                        (F.size(F.array_intersect(F.col('q_ids'),
                                                  F.array([F.lit(x) for x in metadata.get("qualifiers")]))) == F.size(
                            F.array([F.lit(x) for x in metadata.get("qualifiers")]))) &
                        (F.size(F.array_intersect(F.col('q_ids'),
                                                  F.array([F.lit(x) for x in metadata.get("not_qualifiers")]))) == 0) &
                        (F.col('_outcome') == metadata.get("outcome")),
                        1
                    )).alias(column_name) if ('qualifiers' in metadata) and ('outcome' in metadata) and (
                            'not_qualifiers' in metadata) else
                F.count(
                    F.when(
                        (F.col("_type_id").isin(metadata.get('event_type'))) &
                        (F.size(F.array_intersect(F.col('q_ids'),
                                                  F.array([F.lit(x) for x in metadata.get("not_qualifiers")]))) == 0) &
                        (F.size(F.array_intersect(F.col('q_ids'),
                                                  F.array([F.lit(x) for x in metadata.get("qualifiers")]))) == F.size(
                            F.array([F.lit(x) for x in metadata.get("qualifiers")]))),
                        1
                    )).alias(column_name) if ('qualifiers' in metadata) and ('not_qualifiers' in metadata) else
                F.count(
                    F.when(
                        (F.col("_type_id").isin(metadata.get('event_type'))) &
                        (F.size(F.array_intersect(F.col('q_ids'),
                                                  F.array([F.lit(x) for x in metadata.get("not_qualifiers")]))) == 0) &
                        (F.col('_outcome') == metadata.get("outcome")),
                        1
                    )).alias(column_name) if ('outcome' in metadata) and ('not_qualifiers' in metadata) else

                F.count(
                    F.when(
                        (F.col("_type_id").isin(metadata.get('event_type'))) &
                        (F.size(F.array_intersect(F.col('q_ids'),
                                                  F.array([F.lit(x) for x in metadata.get("qualifiers")]))) == F.size(
                            F.array([F.lit(x) for x in metadata.get("qualifiers")]))) &
                        (F.col('_outcome') == metadata.get("outcome")),
                        1
                    )).alias(column_name) if ('outcome' in metadata) and ('qualifiers' in metadata) else
                F.count(
                    F.when(
                        (F.col("_type_id").isin(metadata.get('event_type'))) &
                        (F.size(F.array_intersect(F.col('q_ids'),
                                                  F.array([F.lit(x) for x in metadata.get("qualifiers")]))) == F.size(
                            F.array([F.lit(x) for x in metadata.get("qualifiers")]))),
                        1
                    )).alias(column_name) if ('qualifiers' in metadata) else
                F.count(
                    F.when(
                        (F.col("_type_id").isin(metadata.get('event_type'))) &
                        (F.col('_outcome') == metadata.get("outcome")),
                        1
                    )).alias(column_name) if ('outcome' in metadata) else
                F.count(
                    F.when(
                        (F.col("_type_id").isin(metadata.get('event_type'))) &
                        (F.size(F.array_intersect(F.col('q_ids'),
                                                  F.array([F.lit(x) for x in metadata.get("not_qualifiers")]))) == 0),
                        1
                    )).alias(column_name) if ('not_qualifiers' in metadata) else

                F.count(
                    F.when(
                        (F.col("_type_id").isin(metadata.get('event_type'))),
                        1
                    )).alias(column_name)
                for column_name, metadata in query_dictionary.items()
            ],
            F.avg(F.col('pass_direction')).alias("Pass_direction")
        ).filter(
            F.col('_player_id').isNotNull()
        ).orderBy(
            "_period_id", "_team_id", "_player_id"
        ).withColumn(
            "Pass_direction", F.toDegrees(F.col("Pass_direction"))
        )

        print("Aggregation Done")

        # %% Add proper header for the printing dataframe and pivot table by _Eventname
        printing_df = auto_counters.toPandas()

        printing_df.head()
        printing_df['PlayerType'] = printing_df['_player_id'].map(lambda x: f"p{x}").map(players_dict)

        path = os.path.join("..", "data", "opta_ds")
        if not os.path.exists(path):
            os.makedirs(path)

        printing_df.to_csv(os.path.join(path, "{}_{}.csv".format(folder, file.split('.')[0])), index=False)
        print("File saved")

        '''## SAVING AND PRINTING RESULTS
        # %% Write results to a xlsx file
        with pd.ExcelWriter("data/opta_output/{}/{}.{}".format(folder, file.split('.')[0], 'xlsx')) as writer:
            printing_df.to_excel(writer, sheet_name="Core indicators")
        print('Saved')'''
