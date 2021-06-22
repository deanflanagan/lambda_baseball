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

    df['bet_jays_at_home'] = np.where(df['team'] == 'Toronto Blue Jays', df['team_pl'], np.nan)
    df['multiple_units'] = df.apply(lambda row: bet_multiplier(row, team_indices=(0,9), opposition_indices=(9,18)), axis=1) *  df.apply(lambda row: bet_filter(row, team_indices=(0,9), opposition_indices=(9,18)), axis=1)
    df['bet_single_sd_last_x_games'] = df.apply(lambda row : bet_filter(row),axis=1) * df.apply(lambda row: bet_multiplier(row), axis=1)
    df['bet_single_sd_last_ten_games'] = df.apply(lambda row : bet_filter(row, team_indices=(2,3), opposition_indices=(11,12)),axis=1) * df.apply(lambda row: bet_multiplier(row), axis=1)

    strategy_names = ['bet_jays_at_home', 'multiple_units', 'bet_single_sd_last_x_games', 'bet_single_sd_last_ten_games' ]
    final_columns = ['match_id','match_utc_time','team','opposition', 'home_odds', 'away_odds', 'ft1']
    
    s3 = boto3.resource('s3')
    bucket_name = 'baseball-bets'
    
    for strategy in strategy_names:

        df.drop([x for x in final_columns[-4:] if x != strategy], axis=1)
        strategy_df = df[final_columns + [strategy]].dropna(subset=[strategy])
        # strategy_df.to_json()strategy +'.json', orient='records')
        print(strategy)
        print(strategy_df.to_json())
        object = s3.Object(bucket_name, strategy + '.json')
        object.put(Body=strategy_df.to_json(orient='records'))
        
    return 
        # for game in strategy_df['match_id']:
        #     strategy_df[strategy_df['match_id'] == game].to_json(strategy + game +'.json', orient='records')
