import pandas as pd 
import numpy as np
import boto3

def make_bet_files(df):
    
    calculation_columns = ['team_back_or_lay_six', 'team_back_or_lay_eight',
        'team_back_or_lay_ten', 'team_back_or_lay_six_two_sd',
        'team_back_or_lay_eight_two_sd', 'team_back_or_lay_ten_two_sd',
        'team_back_or_lay_six_three_sd', 'team_back_or_lay_eight_three_sd',
        'team_back_or_lay_ten_three_sd', 'opposition_back_or_lay_six',
        'opposition_back_or_lay_eight', 'opposition_back_or_lay_ten',
        'opposition_back_or_lay_six_two_sd',
        'opposition_back_or_lay_eight_two_sd',
        'opposition_back_or_lay_ten_two_sd',
        'opposition_back_or_lay_six_three_sd',
        'opposition_back_or_lay_eight_three_sd',
        'opposition_back_or_lay_ten_three_sd']

    min_team_odds = 1/df['team_win'].mean()
    max_odds = 2
    df['team_pl'] = np.where(df['ft1'] > df['ft2'], df['home_odds'] -1,  - 1)

    def bet_filter(row, team_indices=(0,3), opposition_indices=(9,12)):
        team_sigs = row[calculation_columns[team_indices[0]:team_indices[1]]].values
        opposition_sigs = row[calculation_columns[opposition_indices[0]:opposition_indices[1]]].values
        if 'back' in team_sigs and row['home_odds'] > min_team_odds and row['home_odds'] < max_odds and 'back' not in opposition_sigs:
            return row['team_pl']
        

    def bet_multiplier(row, team_indices=(0,3), opposition_indices=(9,12)):
        team_sigs = row[calculation_columns[team_indices[0]:team_indices[1]]].values
        opposition_sigs = row[calculation_columns[opposition_indices[0]:opposition_indices[1]]].values
        if sum(~pd.isnull(team_sigs)) > 0 and sum(~pd.isnull(opposition_sigs)) > 0:
            return  sum(~pd.isnull(team_sigs)) + sum(~pd.isnull(opposition_sigs))

    df['bet_jays_at_home_pl'] = np.where(df['team'] == 'Toronto Blue Jays', df['team_pl'], np.nan)
    df['bet_jays_at_home_units'] = np.where(df['team'] == 'Toronto Blue Jays', 1, np.nan)
    df['bet_multiple_strategies_pl'] = df.apply(lambda row: bet_multiplier(row, team_indices=(0,9), opposition_indices=(9,18)), axis=1) *  df.apply(lambda row: bet_filter(row, team_indices=(0,9), opposition_indices=(9,18)), axis=1)
    df['bet_multiple_strategies_units'] = df.apply(lambda row: bet_multiplier(row, team_indices=(0,9), opposition_indices=(9,18)), axis=1) 
    df['bet_single_sd_last_x_games_pl'] = df.apply(lambda row : bet_filter(row),axis=1) * df.apply(lambda row: bet_multiplier(row), axis=1)
    df['bet_single_sd_last_x_games_units'] = df.apply(lambda row: bet_multiplier(row), axis=1)
    df['bet_single_sd_last_ten_games_pl'] = df.apply(lambda row : bet_filter(row, team_indices=(2,3), opposition_indices=(11,12)),axis=1) * df.apply(lambda row: bet_multiplier(row), axis=1)
    df['bet_single_sd_last_ten_games_units'] = df.apply(lambda row: bet_multiplier(row), axis=1)

    final_columns = ['match_id','match_utc_time','team','opposition', 'home_odds', 'away_odds', 'ft1','ft2']

    s3 = boto3.resource('s3')
    bucket_name = 'baseball-bets'
    
    for ind in range(0,8,2):
        strategy_columns = df.columns[ind-8:ind-6]
        strategy_df = df[final_columns + list(strategy_columns)].dropna(subset=strategy_columns)
        if strategy_df.shape[0] > 0:
            object = s3.Object(bucket_name, strategy_columns[0] + '.json')
            object.put(Body=strategy_df.to_json(orient='records'))
            strategy_df.to_json(strategy_columns[0] + '.json',orient='records')
       
    return 

