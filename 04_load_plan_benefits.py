#psycopg2-binary
from config import raw, staging, engine
import pandas as pd
from sqlalchemy import text
from utils import trunc_and_load

# df.info() prints column names, dtypes, and num of present values per col


def load_plan_benefits():
    df = pd.read_sql(text(f"""
        SELECT DISTINCT "Plan Name" AS name,
                        "CDT Code" AS cdt_code,
                        "Service Type" AS service_type,
                        "Network Type" AS network_type,
                        "Copay" AS copay,
                        "Coinsurance" AS coinsurance,
                        "Annual Max Applies" AS annual_max_applies,
                        "Subject To Deductible" AS subject_to_deductible,
                        "Discount Percent" AS discount_percent,
                        "Notes" AS notes
        FROM {raw}."Plan Benefit"
        WHERE "Plan Code" IS NOT NULL 
          AND "Plan Name" IS NOT NULL
          AND "CDT Code" IS NOT NULL 
          AND "Carrier Name" IS NOT NULL
    """), engine)
    
    # Clean values
    # Filter to string columns only (object dtype) to avoid .str.strip() errors on numeric columns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].str.strip()

    df = df.drop_duplicates()

    # Set up Foreign Key References
    cdt_df = pd.read_sql(f"SELECT cdt_code AS code FROM {staging}.procedure", engine)
    plan_df = pd.read_sql(f"SELECT plan_id, name AS p_name FROM {staging}.plan", engine)
    network_type_df = pd.read_sql(f"SELECT network_type_id, name AS nt_name FROM {staging}.network_type", engine)
    service_type_df = pd.read_sql(f"SELECT service_type_id, type AS s_type FROM {staging}.service_type", engine)

    # Resolving FK's
    df = (
        df.merge(cdt_df, how="inner", left_on="cdt_code", right_on="code")
        .drop(columns="code")
        .merge(plan_df, how="inner", left_on="name", right_on="p_name")
        .drop(columns=["name", "p_name"])
        .merge(network_type_df, how="inner", left_on="network_type", right_on="nt_name")
        .drop(columns=["network_type", "nt_name"])
        .merge(service_type_df, how="inner", left_on="service_type", right_on="s_type")
        .drop(columns=["service_type", "s_type"])
    )
    
    # Exclude CDT
    foreign_keys = ["plan_id", "network_type_id", "service_type_id"]
    for key_col in foreign_keys:
        df[key_col] = df[key_col].astype("Int64")

    print("Plan Benefit columns after merge:", df.columns.tolist())

    # Insert values into staging
    insert_sql = text(f"""
        INSERT INTO {staging}.plan_benefit (plan_id, cdt_code, network_type_id, service_type_id, copay, coinsurance, annual_max_applies, subject_to_deductible, discount_percent)
        VALUES (:plan_id, :cdt_code, :network_type_id, :service_type_id, :copay, :coinsurance, :annual_max_applies, :subject_to_deductible, :discount_percent)
    """)

    trunc_and_load(df, "plan_benefit", insert_sql)
    
    print(f"Loaded {len(df)} rows into {staging}.plan_benefits")

load_plan_benefits()