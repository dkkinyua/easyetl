import os
import pandas as pd
from sqlalchemy import create_engine
'''
Load class:
i. Load into a database
ii. Load into a CSV file
iii. Load into a JSON file
iv. Load into an Excel file ( user needs openpyxl module for this )
v. Load into AWS S3 bucket in version 0.2.0
'''
class Load:
    # loads data into a CSV file
    # overwrite = False is a parameter that deny users permissions to overwrite existing files

    # helper function to check if file exists
    @staticmethod
    def _check_file(filepath, overwrite: bool):
        if os.path.exists(filepath) and not overwrite: # checks if filepath already exists in the current directory
            raise FileExistsError(f'The file on {filepath} exists, pass overwrite=True to overwrite file')

    @staticmethod
    def load_csv(data, filepath:str, overwrite: bool = False):
        if not isinstance(data, (pd.DataFrame, pd.Series)): # checks if data is a Pandas DataFrame obj, if not print message
            raise TypeError('Expected data to be a Pandas.DataFrame object')
        
        try:
            Load._check_file(filepath, overwrite)
            data.to_csv(filepath, index=False) # converts data into a CSV file, if it is a Pandas dataframe
            print('CSV file loaded successfully')
        except Exception as e:
            print(f'Error occurred during loading: {str(e)}')

    @staticmethod
    def load_json(data, filepath:str, overwrite: bool=False): # loads data into JSON file
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise TypeError('Expected data to be a Pandas.DataFrame object')
        
        try:
            Load._check_file(filepath, overwrite)
            data.to_json(filepath)
            print('JSON file loaded successfully')
        except Exception as e:
            print(f'Error occurred during loading: {str(e)}')

    @staticmethod
    def load_to_excel(data, filepath:str, overwrite: bool = False): # Loads data into an Excel file
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise TypeError('Expected data to be a Pandas.DataFrame object')
        
        try:
            Load._check_file(filepath, overwrite)
            data.to_excel(filepath)
            print('Excel file loaded successfully')
        except Exception as e:
            print(f'Error occurred during loading: {str(e)}')

    '''
    load_to_db parameters include:
    i. data - Dataframe to be loaded
    ii. name (str) - name of the db - required
    iii. url (str) - Database connection string - required
    '''
    @staticmethod
    def load_to_db(data, name: str, url: str): # loads data into a database
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise TypeError('Expected data needs to be a Pandas.DataFrame object')
        
        try:
            engine = create_engine(url=url) # Creates a connection to the database
            data.to_sql(name=name, con=engine, if_exists='replace') # if table exists, overwrite the table.
            print('Table created successfully!')
        except Exception as e:
            print(f'Error occurred during loading to database: {str(e)}')