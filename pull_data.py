import pandas as pd
import psycopg2

def sql_to_dataframe(conn_string):
    conn = psycopg2.connect(conn_string)
    query = """select * from scraping_game"""
    data_rows = []
    with conn.cursor() as cur:
        cur.execute(query)
        column_names = [desc[0] for desc in cur.description]
        for row in cur:
            data_rows.append(row)
    return pd.DataFrame(data=data_rows, columns=column_names)