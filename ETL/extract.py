import pandas as pd

def extract(csv_file: str):
    """
    Read the CSV file into a pandas DataFrame.
    """
    df = pd.read_csv(csv_file, encoding="latin1")
    print("✅ CSV loaded. Shape:", df.shape)
    return df
