# Example Python program to serialize a pandas DataFrame

from sqlalchemy import create_engine
import os
import sys

import pandas as pd
import psycopg2

def connection():        
    # # postgresql+psycopg2://<user>:<password>@<host>/<db>
    # alchemyEngine = create_engine(
    #     f'postgresql+psycopg2://{user}:{password}@{host}/{db_name}', 
    #     pool_recycle=3600
    # )

    CONN_NAME = os.environ['STAGING_AREA']

    conn = None
    try:
        print('Connecting…')
        conn = psycopg2.connect(
                    host=CONN_NAME['host_name'],
                    database=CONN_NAME['db_name'],
                    user=CONN_NAME['username'],
                    password=CONN_NAME['password']
                )
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("All good, Connection successful!")
    return conn

def sql_to_dataframe(query):
   """
   Import data from a PostgreSQL database using a SELECT query 
   """
   cursor = connection().cursor()
   try:
      cursor.execute(query)
   except (Exception, psycopg2.DatabaseError) as error:
      print(“Error: %s” % error)
   cursor.close()
   return 1
   # The execute returns a list of tuples:
   tuples_list = cursor.fetchall()
   cursor.close()
   # Now we need to transform the list into a pandas DataFrame:
   df = pd.DataFrame(tuples_list)
   print(df)
   return df



def read_data_sql(
    sql, 
    database,
    user,
    password,
    host,
    ):

    try:
        #establishing the connection
        conn = psycopg2.connect(
        database="mydb", user='postgres', password='password', host='127.0.0.1', port= '5432'
        )

        #Setting auto commit false
        conn.autocommit = True

        #Creating a cursor object using the cursor() method
        cursor = conn.cursor()

        #Retrieving data
        cursor.execute(sql)

        #Fetching 1st row from the table
        result = cursor.fetchone();
        print(result)

        #Fetching 1st row from the table
        result = cursor.fetchall();
        print(result)

        #Commit your changes in the database
        conn.commit()

        #Closing the connection
        conn.close()
    

    return result