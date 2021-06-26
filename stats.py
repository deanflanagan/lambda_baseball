import pandas as pd 
import numpy as np


def add_signals(df):
    df['team_win'] = np.where(df.ft1 > df.ft2, 1,0)
    df['opposition_win'] = np.where(df.ft2 > df.ft1, 1,0)

    df['team_fair_odds'] = np.reciprocal(df['home_odds']) / (np.reciprocal(df['home_odds'])   + np.reciprocal(df['away_odds']) )
    df['opposition_fair_odds'] = np.reciprocal(df['away_odds']) / (np.reciprocal(df['home_odds'])   + np.reciprocal(df['away_odds']) )

    df['team_market_error'] = df['team_win'] - df['team_fair_odds']
    df['opposition_market_error'] = df['opposition_win'] - df['opposition_fair_odds']

    df['team_runline'] = df['ft1'] - df['ft2']
    df['opposition_runline'] = df['ft2'] - df['ft1']

    df1 = df[[ 'match_id','match_utc_time',
        'match_status', 'team',
        'home_odds',  'team_win', 
        'team_fair_odds',  'team_market_error','team_runline'
        ]]

    df1['is_home'] = 1

    df2 = df[[ 'match_id','match_utc_time',
        'match_status', 'opposition',
        'away_odds',  'opposition_win', 
        'opposition_fair_odds',  'opposition_market_error','opposition_runline'
        ]]
    df2['is_home'] = 0

    arr = pd.DataFrame(np.concatenate((df1.values, df2.values)), 
        columns=[ 'match_id','match_utc_time',
        'match_status', 'team',
        'odds',  'win', 
        'fair_odds',  'market_error','runline','is_home'
        ])

    arr.sort_values('match_utc_time',inplace=True)

    game_count = []
    trailing_six_games = []
    trailing_eight_games = []
    trailing_ten_games = []

    for ind in arr.index:
        team = arr['team'][ind]
        date_cutoff = arr['match_utc_time'][ind]

        game_count.append(arr[(arr['team'] == team)&(arr['match_utc_time'] < date_cutoff) & (arr['match_status'] == 'ft')].shape[0])
        trailing_six_games.append(arr[(arr['team'] == team)&(arr['match_utc_time'] < date_cutoff) & (arr['match_status'] == 'ft')]['market_error'].values[-6:].sum())
        trailing_eight_games.append(arr[(arr['team'] == team)&(arr['match_utc_time'] < date_cutoff)& (arr['match_status'] == 'ft')]['market_error'].values[-8:].sum())
        trailing_ten_games.append(arr[(arr['team'] == team)&(arr['match_utc_time'] < date_cutoff)& (arr['match_status'] == 'ft')]['market_error'].values[-10:].sum())


    arr['game_count'] = game_count
    arr['error_last_six'] = trailing_six_games
    arr['error_last_eight'] = trailing_eight_games
    arr['error_last_ten'] = trailing_ten_games

    std_six = np.std(arr[arr['match_status'] == 'ft']['error_last_six'].astype('float'))
    std_eight = np.std(arr[arr['match_status'] == 'ft']['error_last_eight'].astype('float'))
    std_ten = np.std(arr[arr['match_status'] == 'ft']['error_last_ten'].astype('float'))

    arr['back_or_lay_six'] = np.where(arr['error_last_six'] > std_six, 'lay', np.where(arr['error_last_six'] < -std_six, 'back', ''))
    arr['back_or_lay_eight'] = np.where(arr['error_last_eight'] > std_eight, 'lay', np.where(arr['error_last_eight'] < -std_eight, 'back', ''))
    arr['back_or_lay_ten'] = np.where(arr['error_last_ten'] > std_ten, 'lay', np.where(arr['error_last_ten'] < -std_ten, 'back', ''))

    arr['back_or_lay_six_two_sd'] = np.where(arr['error_last_six'] > std_six * 2, 'lay', np.where(arr['error_last_six'] < -std_six *2, 'back', ''))
    arr['back_or_lay_eight_two_sd'] = np.where(arr['error_last_eight'] > std_eight *2 , 'lay', np.where(arr['error_last_eight'] < -std_eight *2, 'back', ''))
    arr['back_or_lay_ten_two_sd'] = np.where(arr['error_last_ten'] > std_ten *2, 'lay', np.where(arr['error_last_ten'] < -std_ten *2, 'back', ''))

    arr['back_or_lay_six_three_sd'] = np.where(arr['error_last_six'] > std_six *3, 'lay', np.where(arr['error_last_six'] < -std_six *3, 'back', ''))
    arr['back_or_lay_eight_three_sd'] = np.where(arr['error_last_eight'] > std_eight * 3, 'lay', np.where(arr['error_last_eight'] < -std_eight *3, 'back', ''))
    arr['back_or_lay_ten_three_sd'] = np.where(arr['error_last_ten'] > std_ten * 3, 'lay', np.where(arr['error_last_ten'] < -std_ten *3, 'back', ''))

    df3 = arr[arr['is_home'] == 1][['match_id','back_or_lay_six', 'back_or_lay_eight', 'back_or_lay_ten',
        'back_or_lay_six_two_sd', 'back_or_lay_eight_two_sd',
        'back_or_lay_ten_two_sd', 'back_or_lay_six_three_sd',
        'back_or_lay_eight_three_sd', 'back_or_lay_ten_three_sd']]

    df3.columns = list(df3.columns[:1]) + ['team_' +x for x in  df3.columns[1:]] 

    df4 = arr[arr['is_home'] == 0][['match_id','back_or_lay_six', 'back_or_lay_eight', 'back_or_lay_ten',
        'back_or_lay_six_two_sd', 'back_or_lay_eight_two_sd',
        'back_or_lay_ten_two_sd', 'back_or_lay_six_three_sd',
        'back_or_lay_eight_three_sd', 'back_or_lay_ten_three_sd']]

    df4.columns = list(df4.columns[:1]) + ['opposition_' +x for x in  df4.columns[1:]] 

    df5 = pd.merge(df3, df4, on='match_id')
    
    return pd.merge(df.drop_duplicates('match_id', keep='last'), df5.drop_duplicates('match_id',keep='last'), on='match_id').sort_values('match_utc_time')
