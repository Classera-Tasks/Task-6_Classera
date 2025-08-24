import pandas as pd
from sqlalchemy import create_engine, text

csv_file = "D:/sales_data_sample.csv"
df = pd.read_csv(csv_file, encoding="latin1")

print("✅ CSV loaded. Shape:", df.shape)

df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
df.dropna(how="all", inplace=True)

for col in df.select_dtypes(include=['number']).columns:
    df[col] = df[col].fillna(0)

for col in df.select_dtypes(include=['object']).columns:
    df[col] = df[col].fillna("Unknown")

date_columns = [col for col in df.columns if "date" in col.lower()]
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors="coerce")
    df[col] = df[col].fillna(pd.Timestamp("1970-01-01"))

df.drop_duplicates(inplace=True)

print("✅ Data cleaned. Shape:", df.shape)

username = "postgres"
password = "12345678"
host = "localhost"
port = "5432"
database = "salesdb"

engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}")

table_name = "sales_data"
df.to_sql(table_name, engine, if_exists="replace", index=False)

print("✅ Data inserted into PostgreSQL table:", table_name)

with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
    row_count = result.scalar()
    print("📊 Row count in DB:", row_count)

    result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 10;"))
    print("\n🔎 Sample data from DB:")
    for row in result:
        print(row)
