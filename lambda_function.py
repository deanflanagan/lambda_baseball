import numpy as np
import pandas as pd 
import os
import json 
from pull_data import sql_to_dataframe
from stats import add_signals
from add_bets import make_bet_files

rds_host  = os.getenv('RDS_HOST')
rds_username = os.getenv('RDS_USERNAME')
rds_user_pwd = os.getenv('RDS_USER_PWD')
rds_db_name = os.getenv('RDS_DB_NAME')


def lambda_handler(event, context):
    conn_string = "host=%s user=%s password=%s dbname=%s" % \
                    (rds_host, rds_username, rds_user_pwd, rds_db_name)
            
    print('starting sql extraction')
    df = sql_to_dataframe(conn_string=conn_string)
    print('done the connection to rds')
    df = add_signals(df)
    print('signals added')
    make_bet_files(df)
    # import boto3
    # s3 = boto3.resource('s3')
    # bucket_name = 'baseball-bets'
    

    # object = s3.Object(bucket_name, 'tryme.json')
    # object.put(Body=json.dumps({"value":"key"}).encode())

    return {'status':200}