import pandas as pd

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