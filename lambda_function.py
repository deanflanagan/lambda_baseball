import os
from pull_data import sql_to_dataframe
from stats import add_signals
from add_bets import make_bet_files

rds_host  = os.getenv('RDS_HOST')
rds_username = os.getenv('RDS_USERNAME')
rds_user_pwd = os.getenv('RDS_USER_PWD')
rds_db_name = os.getenv('RDS_DB_NAME')

# rds_host  = 'database-1.comg0aeojiea.ca-central-1.rds.amazonaws.com'
# rds_username = 'postgres'
# rds_user_pwd = '02022729'
# rds_db_name = 'postgres'


def lambda_handler(event, context):
    conn_string = "host=%s user=%s password=%s dbname=%s" % \
                    (rds_host, rds_username, rds_user_pwd, rds_db_name)
            
    print('starting sql extraction')
    df = sql_to_dataframe(conn_string=conn_string)
    print('done the connection to rds')
    df = add_signals(df)
    print('signals added')
    make_bet_files(df)

    return {'status':200}
