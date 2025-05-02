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
    def read_api(url: str, headers: dict = None):
        default_kwargs = {
            'headers': headers
        }
        response = requests.get(url, **default_kwargs)
        response.raise_for_status()
        return response.json()
    
    # Extracts data from a PostgreSQL database
    '''
    Read data from a SQL database via URL
    i. url: A database URI to connect to, dtype: str
    ii. table_name: The name of the table to be read, dtype:str
    iii. columns: The columns to be read from the database, dtype:list
    iv. parse_date: The columns to be parsed into datetime columns, dtype: list/dict.
    '''
    @staticmethod
    def read_db_via_url(url: str, table_name: str, columns: list = None, parse_dates: list | dict = None):
        try:
            default_kwargs = {
                'columns': columns,
                'parse_dates': parse_dates
            }

            data = pd.read_sql_table(table_name, con=url, **default_kwargs)
            return data
        except Exception as e:
            print(f"Error: {str(e)}")

        
    '''
    Access database via database parameters. For PostgreSQL databases!
    i. query: A SQL query to show data from database e.g. 'SELECT * FROM employees'
    '''
    @staticmethod
    def read_db_via_params(database:str, user:str, password:str, query, host:str='localhost', port:str='5432'):
        try:
            connection = psycopg2.connect(
                database=database,
                user=user,
                password=password,
                host=host,
                port=port
            )
            data = pd.read_sql(query, connection)
            connection.close() # close connection as data is already retrieved
            return data
        except Exception as e:
            print(f"Error: {str(e)}")

