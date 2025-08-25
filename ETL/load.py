from sqlalchemy import create_engine, text
import pandas as pd

def load(df: pd.DataFrame, db_config: dict, table_name: str = "sales_data"):
    """
    Create table if not exists and insert DataFrame into PostgreSQL
    """
    engine = create_engine(
        f"postgresql+psycopg2://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )

    # SQL for table schema
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sales_data (
        ORDERNUMBER INT,
        QUANTITYORDERED INT,
        PRICEEACH NUMERIC(10,2),
        ORDERLINENUMBER INT,
        SALES NUMERIC(12,2),
        ORDERDATE DATE,
        STATUS VARCHAR(50),
        QTR_ID INT,
        MONTH_ID INT,
        YEAR_ID INT,
        PRODUCTLINE VARCHAR(100),
        MSRP INT,
        PRODUCTCODE VARCHAR(50),
        CUSTOMERNAME VARCHAR(255),
        PHONE VARCHAR(50),
        ADDRESSLINE1 VARCHAR(255),
        ADDRESSLINE2 VARCHAR(255),
        CITY VARCHAR(100),
        STATE VARCHAR(100),
        POSTALCODE VARCHAR(20),
        COUNTRY VARCHAR(100),
        TERRITORY VARCHAR(100),
        CONTACTLASTNAME VARCHAR(100),
        CONTACTFIRSTNAME VARCHAR(100),
        DEALSIZE VARCHAR(20),
        PRIMARY KEY (ORDERNUMBER, ORDERLINENUMBER)
    );
    """

    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        print("✅ Table 'sales_data' created or already exists")

    # Insert data
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"✅ Data inserted into table '{table_name}'")

    # Validation
    with engine.connect() as conn:
        row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};")).scalar()
        print(f"📊 Row count in DB: {row_count}")
