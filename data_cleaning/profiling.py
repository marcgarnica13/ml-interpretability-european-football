import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.graph_objects as go
data_folder_path = os.path.join('gender_analysis', 'data')
data_file = 'ds.csv'

df = pd.read_csv(os.path.join(data_folder_path, data_file))
#%%
feature_space = [c for c in df.columns if c not in [
    'matchPeriod', 'gender', 'PlayerType', 'Goals',
    'Own_goals', 'Goals_from_penalty', 'Successful_crosses',
    'Unsuccessful_crosses', 'Corners', 'Corners_successful', 'Corners_unsuccessful',
    'Saves', 'Shots_on_target', 'Shots_off_target'
]]
print(feature_space)
number_of_cols = 4
number_of_rows = 5
fig = make_subplots(rows=number_of_rows, cols=number_of_cols, subplot_titles=feature_space)

df['gender'] = df['gender'].replace(0, 'Female')
df['gender'] = df['gender'].replace(1, 'Male')
i = 0
for f in feature_space:
    row = int(i / number_of_cols) + 1
    col = (i % number_of_cols) + 1
    print(row, col)
    aux_fig = px.box(df, x=f, y='gender', color='gender', color_discrete_map={
        'Male': 'rgba(93, 164, 214, 0.5)',
        'Female': 'rgba(255, 144, 14, 0.5)'
    })
    if row != number_of_rows or col != number_of_cols:
        aux_fig.data[0]['showlegend'] = False
        aux_fig.data[1]['showlegend'] = False
    fig.add_trace(aux_fig.data[0], col=col, row=row)
    fig.add_trace(aux_fig.data[1], col=col, row=row)
    i += 1

fig.update_yaxes(visible=False)

fig.update_layout(
    legend=dict(
        orientation="h",
        yanchor="bottom",
        xanchor='right',
        y=1.02,
        x=1
    ),
)

fig.update_layout(
    xaxis=dict(zeroline=False),
    width=2000,
    height=2000,
    boxgap=0.1,
    yaxis=dict(
        visible=False,
        showgrid=True,
        gridcolor='rgb(243, 243, 243)',
        zerolinecolor='rgb(243, 243, 243)',
    ),
    margin=dict(
        l=40,
        r=30,
        b=80,
        t=100,
    ),
    paper_bgcolor='lightgrey',
    plot_bgcolor='rgb(255, 255, 255)',
    showlegend=True,
    font=dict(
        family="Helvetica, sans-serif",
        size=24,
    )
)

for i in fig['layout']['annotations']:
    i['font'] = dict(size=24)

fig.show()

'''fig = go.Figure()
df_female = df[df['gender'] == 0]
for feature in feature_space:
    fig.add_trace(go.Box(
        y=df_female[feature],
        x=np.full(shape=(df_female.shape[0]), fill_value=feature),
        marker_color='rgba(93, 164, 214, 0.5)',
    ))
df_male = df[df['gender'] == 1]
for feature in feature_space:
    fig.add_trace(go.Box(
        y=df_male[feature],
        x=np.full(shape=(df_male.shape[0]), fill_value=feature),
        marker_color='rgba(255, 144, 14, 0.5)',
    ))

fig.show()'''
