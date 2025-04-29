import requests
import pandas as pd
import psycopg2

'''
@staticmethod -> A decorator used to declare methods in classes, that do not need a `self` instance or `cls`. There are regular functions defined inside functions for logical grouping
'''
class Extract:
    # Extracts data from a CSV file
    @staticmethod
    def read_csv(filepath: str):
        data = pd.read_csv(filepath)
        return data
    
    # Extracts data from a JSON file
    @staticmethod
    def read_json(filepath: str):
        data = pd.read_json(filepath)
        return data
    
    # Extracts data from an API
    @staticmethod
    def read_api(url: str):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    # Extracts data from a PostgreSQL database
    @staticmethod
    def read_db(database:str, url:str, username:str, password:str, query, host:str='localhost', port:str='5432'):
        connection = psycopg2.connect(
            database = database,
            username = username,
            password = password,
            host= host,
            port = port
        )
        data = pd.read_sql_query(query, connection) # Reads the data pulled from the db connection
        connection.close() # Closes connection to the db as data is retrieved already
        return data

