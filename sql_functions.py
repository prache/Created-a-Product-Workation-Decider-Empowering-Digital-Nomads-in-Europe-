# We import a method from the  modules to address environment variables and 
# we use that method in a function that will return the variables we need from .env 
# to a dictionary we call sql_config
import sqlalchemy
from dotenv import dotenv_values

def get_sql_config():
    '''
        Function loads credentials from .env file and
        returns a dictionary containing the data needed for sqlalchemy.create_engine()
    '''
    needed_keys = ['host', 'port', 'database','user','password']
    dotenv_dict = dotenv_values(".env")
    sql_config = {key:dotenv_dict[key] for key in needed_keys if key in dotenv_dict}
    #print(sql_config)
    return sql_config

# Import sqlalchemy and pandas - do this only when instructed

# Insert the get_data() function definition below - do this only when instructed in the notebook
# Write the get data function
''' Connect to the PostgreSQL database server, run query and return data'''
# get the connection configuration dictionary using the get_sql_config function
# create a connection engine to the PostgreSQL server
# open a conn session using 'with', execute the query, and return the results
def get_data(sql_query):
    sql_config = get_sql_config()
    engine = sqlalchemy.create_engine('postgresql://user:pass@host/database',
                        connect_args= sql_config # use dictionary with config details
                            )
    with engine.begin() as conn:
        results = conn.execute(sql_query)
        print(results.fetchall())

      

# Insert the get_dataframe() function definition below - do this only when instructed in the notebook
# define a new function get_dataframe() based on format of get_data() but using read_sql_query()
import pandas as pd
import sqlalchemy 

def get_dataframe(sql_query):
# get the connection configuration dictionary using the get_sql_config function
    sql_config = get_sql_config()
    # create a connection engine to the PostgreSQL server
    engine = sqlalchemy.create_engine('postgresql://user:pass@host/database',
                            connect_args= sql_config # use dictionary with config details
                            )
    return pd.read_sql_query(sql=sql_query, con=engine)

   

# Insert the get_engine() function definition below - when instructed
# function to create sqlalchemy engine for writing data to a database

def get_engine():
    sql_config = get_sql_config()
    engine = sqlalchemy.create_engine('postgresql://user:pass@host/database',
                        connect_args=sql_config
                        )
    return engine     