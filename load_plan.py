# psycopg2-binary
from config import raw, staging, engine
import pandas as pd
from sqlalchemy import text
from utils import trunc_and_load


def load_plan():
    df = pd.read_sql(text(f"""
        SELECT DISTINCT "Carrier Name" AS carrier_name,
                        "Plan Name" AS name,
                        "Plan Code" AS code,
                        "Plan Type" AS plan_type,
                        "Plan Category" AS plan_category,
                        "Plan Cost" AS cost
        FROM {raw}."Plan"
        WHERE "Plan Code" IS NOT NULL
    """), engine)

    # Clean values
    # Filter to string columns only (object dtype) to avoid .str.strip() errors on numeric columns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].str.strip()

    df = df.drop_duplicates()

    # Setting up foreign key references
    carrier_df = pd.read_sql(f"SELECT carrier_id, name AS c_name FROM {staging}.carrier", engine)
    plan_type_df = pd.read_sql(f"SELECT plan_type_id, name AS pt_name FROM {staging}.plan_type", engine)
    plan_category_df = pd.read_sql(f"SELECT plan_category_id, category FROM {staging}.plan_category", engine)

    # Resolving FK's
    df = (
        df.merge(carrier_df, how="inner", left_on="carrier_name", right_on="c_name")
        .drop(columns=["carrier_name", "c_name"])
        .merge(plan_type_df, how="inner", left_on="plan_type", right_on="pt_name")
        .drop(columns=["plan_type", "pt_name"])
        .merge(plan_category_df, how="inner", left_on="plan_category", right_on="category")
        .drop(columns=["plan_category", "category"])
    )

    foreign_keys = ["carrier_id", "plan_type_id", "plan_category_id"]
    for key_col in foreign_keys:
        df[key_col] = df[key_col].astype("Int64")

    print("Columns after merges:", df.columns.tolist())

    # Insert values into staging
    insert_sql = text(f"""
        INSERT INTO {staging}.plan (carrier_id, name, code, plan_type_id, plan_category_id, cost)
        VALUES (:carrier_id, :name, :code, :plan_type_id, :plan_category_id, :cost)
    """)

    # Custom function to load
    trunc_and_load(df, "plan", insert_sql)

    print(f"Loaded {len(df)} rows into {staging}.plan")

load_plan()