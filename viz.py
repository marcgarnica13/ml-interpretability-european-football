import pandas as pd
import plotly.express as px
import os

folder_path = os.path.join('/', 'home', 'marc', 'Development', 'dshs', 'gender_analysis', 'classification')
#%%
data_file = 'coef.csv'
df = pd.read_csv(os.path.join(folder_path, data_file))
fig = px.bar(
    df[df['Feature'].isin(
        [
            'matchPeriod_1H', 'matchPeriod_2H',
            'matchPeriod_E1', 'matchPeriod_E2',
            'matchPeriod_P'
        ]
    )].sort_values(by=['Odds ratio'], ascending=False),
    x='Feature',
    y='Odds ratio',
    title='Game sections'
)
fig.show()