from extract import extract
from transform import transform
from load import load

# CSV file path
csv_file = "../sales_data_sample.csv"

# Database config
db_config = {
    "username": "postgres",
    "password": "12345678",
    "host": "localhost",
    "port": "5432",
    "database": "salesdb"
}

# ETL process
df = extract(csv_file)
df = transform(df)
load(df, db_config)
