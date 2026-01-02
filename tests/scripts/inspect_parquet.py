import pandas as pd
import pyarrow.parquet as pq
import sys
import os

def inspect_parquet(file_path):
    print(f"Inspecting File: {file_path}")
    table = pq.read_table(file_path)
    print("\n=== Schema ===")
    print(table.schema)
    
    print("\n=== Row Count ===")
    print(f"Rows: {table.num_rows}")
    
    print("\n=== First 2 Rows (Native Types) ===")
    df = table.to_pandas()
    print(df.head(2).to_dict(orient='records'))
    print("\n=== Pandas Types ===")
    print(df.dtypes)

if __name__ == "__main__":
    dir_path = sys.argv[1]
    for f in os.listdir(dir_path):
        if f.endswith(".parquet"):
            inspect_parquet(os.path.join(dir_path, f))
            break # Just check one chunk
