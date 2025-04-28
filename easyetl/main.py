import os
import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
# class Extract is used for extract data from various sources, APIs, JSON files, CSV files and databases.
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

'''
Load class:
i. Load into a database
ii. Load into a CSV file
iii. Load into a JSON file
iv. Load into an Excel file ( user needs openpyxl module for this )
v. Load into an API through requests.post
vi. Load into AWS S3 bucket 
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


"""
Transform:
i. drop NA rows and columns
ii replacing values
iii. explode df into individual rows
iv: change column dtype
v: change into a datetime instance
"""
class Transform:
    '''
    Removes missing values. 
    Takes in 5 parameters, 1 mandatory, 4 optional.
    i. data = a pandas.DataFrame object
    ii. drop = checks to drop either index(rows) or column, default index
    iii. inplace = Save the dataframe without the missing values, default False
    iv. columns = A list of columns to check and remove missing values, optional param
    v. how = Tells our method on what parameters should we drop missing values.'all' drops a row/column if it contains all NA values and 'any' drops any row/column with a missing value. Default any
    '''
    @staticmethod
    def drop_na(data, columns: list=None, drop: str='index', inplace: bool=False, how: str='any'): 
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise TypeError('Expected data to be a pandas.DataFrame')
        
        try:
            default_kwargs = {
                'axis': drop,
                'how': how,
                'inplace': inplace
            }

            if columns is not None:
                default_kwargs['subset'] = columns

            cleaned_data = data.dropna(**default_kwargs) # keeps code clean with a dict of default arguments using keyword arguments **kwargs
            
            if not inplace: # if inplace = False, return the cleaned data
                return cleaned_data
        except Exception as e:
            print(f'Error: {str(e)}')   

    '''
    Replaces an existing value with a new value.
    i. data: this is a pd.DataFrame object.
    ii. item_a: the item to be replaced. Takes in a single item, a list of values or a dictionary of key-value pairs. The dtype of item_a has not been defined
    iii. item_b: the item to replace item_a. Takes in a single item, a list of values or a dictionary of key-value pairs. The dtype of item_b has not been defined

    Uses pd.DataFrame.replace() method from pandas
    Example usage: df = df.replace()
    '''
    @staticmethod
    def replace(data, item_a, item_b, inplace: bool=False):
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise TypeError('Expected data to be a pandas.DataFrame')
        
        try:
            clean_data = data.replace(item_a, item_b, inplace=inplace)
            return clean_data if not inplace else data
        except Exception as e:
            print(f'Error: {str(e)}')

    '''
    Explodes rows in a list into individual rows.
    i. data: a pd.DataFrame
    ii. columns: a list of columns to be exploded

    Example usage: df = df.explode(['A', 'B', 'C'])
    '''

    @staticmethod
    def explode(data, columns):
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise TypeError('Expected data to be a Dataframe')
        
        try:
            clean_data = data.explode(columns)
            return clean_data
        except Exception as e:
            print(f'Error: {str(e)}')

    '''
    Changes the type of a column or dataframe
    i. data: Should a pd.DataFrame or pd.Series
    ii. dtype: Datatype, float, int, str etc

    Example usage: df['A'] = df['A'].changetype(str)
    '''
    @staticmethod
    def changetype(data, dtype):
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise TypeError('Expected data to be a Dataframe or Series')
        
        try:
            result = data.astype(dtype)
            return result
        except Exception as e:
            print(f'Error: {str(e)}')

    '''
    changes datetime str values into datetime objects to work with time series data
    i. data: Should be a dataframe or series, mostly a series to convert a column into a datetime object
    ii. column: The column we want to convert into a datetime object
    Example usage = df['dates'] = df.to_datetime(df['dates'])
    '''
    @staticmethod
    def to_datetime(data, column):
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise TypeError('Expected data to be a Dataframe or Series')
        
        if not column or column not in data.columns:
            raise ValueError('Column is not in data. Please pass a valid column name')
        
        try:
            df = data.copy()
            df[column] = pd.to_datetime(df[column])
            return df
        except Exception as e:
            print(f'Error: {str(e)}')

    '''
    renames column or index labels
    i. data: a dataframe
    ii. to_rename: specifies whether to rename a column or index (same with axis in pandas)
    iii. columns: a dictionary with a key-value pair of {'old_column_label': 'new_column_label'}
    iv. index: a dictionary with a key-value pair of {'old_index_label': 'new_index_label'}
    v. inplace: Modifies data permanently. Default = False and returns the modified dataframe, True returns the unmodified dataframe as it returns None in pandas
    vi. errors: returns errors. Default = 'ignore', ignores errors and returns existing keys will be renamed and extra keys will be ignored, 'raise' will raise a KeyError to indicate that columns/index args have values that are not present in the dataframe.
    '''
    def rename(data, to_rename: str, columns: dict, index: dict, inplace: bool=False, errors: str='ignore'):
        if not isinstance(data, (pd.DataFrame)):
            raise TypeError('Expected data to be a Dataframe')
        
        try:
            default_kwargs = {
                'axis': to_rename,
                'columns': columns,
                'index': index,
                'inplace': inplace,
                'errors': errors
            }
            df = data.rename(**default_kwargs)
            return df if not inplace else data
        except Exception as e:
            print(f'Error: {str(e)}')