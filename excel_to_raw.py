# psycopg2-binary
from config import DATABASE_URL, excel_path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(DATABASE_URL)
xlsx = pd.ExcelFile(excel_path)

exclude_sheets = ["Fact Sheet ->", "Reference Sheets ->", "README", "Yes No"]

for sheet in xlsx.sheet_names:
    if sheet not in exclude_sheets:
        df = pd.read_excel(excel_path, sheet_name=sheet)
        df.to_sql(
            name=sheet, # Same name as sheet name
            con=engine,
            schema="raw",
            if_exists="replace",
            index=False
        )
        print(f"{sheet} successfully loaded")

timestamp_df = pd.DataFrame([{"load_time": datetime.now()}])
timestamp_df.to_sql(
    name="load_timestamp",
    con=engine,
    schema="raw",
    if_exists="replace",
    index=False
)

print("All sheets loaded")