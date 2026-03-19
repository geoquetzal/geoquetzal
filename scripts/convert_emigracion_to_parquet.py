"""
One-time script: convert MIGRACION_BDP.csv -> emigracion.parquet

Upload emigracion.parquet to:
  https://github.com/geoquetzal/censo2018/releases/tag/emigracion-v1.0

Usage:
    python convert_emigracion_to_parquet.py MIGRACION_BDP.csv
"""
import sys, os
import pandas as pd

csv_path = sys.argv[1] if len(sys.argv) > 1 else "MIGRACION_BDP.csv"
df = pd.read_csv(csv_path)
for col in df.columns:
    df[col] = pd.to_numeric(df[col], downcast="integer")

out = "emigracion.parquet"
df.to_parquet(out, index=False, engine="pyarrow", compression="snappy")

print(f"Rows:    {len(df):,}")
print(f"CSV:     {os.path.getsize(csv_path) / 1024 / 1024:.2f} MB")
print(f"Parquet: {os.path.getsize(out) / 1024 / 1024:.2f} MB")
