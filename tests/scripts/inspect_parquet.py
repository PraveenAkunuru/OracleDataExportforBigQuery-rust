# Copyright 2026 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
