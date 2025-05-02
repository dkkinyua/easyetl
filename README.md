# eazyetl

## Introduction

eazyetl is a lightweight, beginner-friendly, and modular Python package for building end-to-end ETL (Extract, Transform, Load) pipelines. It provides intuitive classes and methods for working with data from various sources like APIs, CSV/JSON files, and databases, and helps you clean, transform, and load that data with ease.

## Key changes.

In version 0.1.6, two new methods have been added to the `Extract()` class.
- `Extract.read_db_via_url` and
- `Extract.read_db_via_params`

`Extract.read_db_via_url` reads a SQL database e.g. MySQL or PostgreSQL via a database URI. This is easier and cleaner to use as compared to `Extract.read_db_via_params` which only reads a PostgreSQL database and entering those database values seems quite cumbersome for the user, but it is still included for users who might prefer this to using URIs.

i. `Extract.read_db_via_url` parameters:
- `url` datatype, str. The database URI, required parameter
- `table_name` datatype, str. The table name, required parameter
- `columns` datatype, list. A list of columns from the table
- `parse_dates` datatype, list or dict. This parameter tells our function which columns to parse as datetime objects.

ii. `Extract.read_db_via_params` parameters:
- `database` datatype str. The database name
- `query` datatype str. The SQL query to be executed when running the method. Example: `'SELECT * FROM employees'`
- `user` datatype str. The user's username
- `password` datatype str. The user's password
- `host` datatype str. The database's host, default localhost
- `port` datatype str. The database's port number, default 5432

## Installation
Install the package from TestPyPI:
```bash
pip install eazyetl
```
## Features
- 📦 Extract from CSV, JSON, APIs, and PostgreSQL
- 🧹 Transform using common operations like dropna, replace, explode, to_datetime, and rename

- 📂 Load into CSV, JSON, Excel, PostgreSQL databases

- ☁️ Modular, static-method design (no complex setup required)

- 🐍 Designed with Pandas and SQLAlchemy for powerful data handling

## Usage

**a. Import the `eazyetl` library**
```python
from eazyetl import Extract, Transform, Load
```

**b. Extract data from various sources using the `Extract()` methods**

*NOTE: The `Extract.read_db()` will include a database URL parameter to connect to databases more seamlessly rather than entering credentials which is more tiring. This will be available in version 0.2.0*

*NOTE: Version 0.2.0 will also contain a `Extract.read_bucket()` method which will enable users to read data from Amazon Web Services (AWS) Simple Storage Services (S3) buckets.*

```python
df = Extract.read_csv("data/data.csv")
api_data = Extract.read_api(url= 'https://fantasypremierleague.com/users/data') # not a real URL
db_data = Extract.read_db(database='employees', user='postgres', password='postgressuperuser', host='localhost', port='5432')
```

**c. Transform data**

```python
df = Transform.drop_na(df, columns=["name", "price"])
df = Transform.to_datetime(df, "release_date")
df = Transform.rename(df, columns={"old_name": "new_name"})
```

**d. Load data**
```python
Load.load_csv(df, "cleaned_data.csv", overwrite=True)
Load.load_to_excel(df, 'weather_data.xlsx', overwrite=False)
Load.load_to_db(df, name="salaries", url="postgresql://user:pass@localhost:5432/mydb")
```
## Documentation

### 1. Extract

| Method | Description |
|--------|-------------|
| `read_csv(filepath)` | Load data from CSV |
| `read_json(filepath)` | Load data from JSON |
| `read_api(url)` | Load JSON data from an API |
| `read_db_via_params(query, user, password, host, port, database)` | Load data from PostgreSQL database |
| `read_db_via_url(url, table_name, columns=None, parse_dates=None)` | Load data from database URI |

### 2. Transform

| Method | Description |
|--------|-------------|
| `drop_na(data, columns=None, drop='index', inplace=False, how='any')` | Drop missing values |
| `replace(data, item_a, item_b, inplace=False)` | Replace values |
| `explode(data, columns)` | Explode rows containing lists |
| `changetype(data, dtype)` | Change column or Series data type |
| `to_datetime(data, column)` | Convert column to datetime format |
| `rename(data, columns=None, index=None, inplace=False)` | Rename columns or index |

### 3. Load

| Method | Description |
|--------|-------------|
| `load_csv(data, filepath, overwrite=False)` | Save data to CSV |
| `load_json(data, filepath, overwrite=False)` | Save data to JSON |
| `load_to_excel(data, filepath, overwrite=False)` | Save data to Excel (requires `openpyxl`) |
| `load_to_db(data, name, url)` | Save data to PostgreSQL table |

## Requirements.

These will be automatically installed by running the `pip install eazyetl` command.

- Python 3.7+

- pandas

- requests

- sqlalchemy

- psycopg2-binary

- openpyxl (for Excel file export)

## Author

**Name: Denzel 'deecodes' Kinyua**

*Data Engineer*

**GitHub: https://github.com/dkkinyua**

**Portfolio: https://denzel-kinyua.vercel.app**

**Email: denzelkinyua11@gmail.com**

## License
This project is licensed under the MIT License.

## Contributions
Pull requests are welcome! If you'd like to suggest a feature or report a bug, open an issue on GitHub.
