import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the DataFrame:
    - Strip strings
    - Fill missing values
    - Convert dates
    - Remove duplicates
    """
    # Strip whitespace
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Drop rows where all values are missing
    df.dropna(how="all", inplace=True)

    # Fill numeric NaNs with 0
    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].fillna(0)

    # Fill string NaNs with "Unknown"
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna("Unknown")

    # Handle date columns
    date_columns = [col for col in df.columns if "date" in col.lower()]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df[col] = df[col].fillna(pd.Timestamp("1970-01-01"))

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    print("✅ Data transformed. Shape:", df.shape)
    return df
