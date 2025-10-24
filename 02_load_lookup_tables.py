# psycopg2-binary
from config import raw, staging, engine
import pandas as pd
from sqlalchemy import text
from utils import trunc_and_load


def load_lookup(raw_table, staging_table, raw_col, staging_col):

    q = text(f'SELECT DISTINCT "{raw_col}" AS val FROM {raw}."{raw_table}" WHERE "{raw_col}" IS NOT NULL')
    
    # Cleaning column values
    df = pd.read_sql(q, engine)
    df["val"] = df["val"].str.strip()
    df = df.query('val != ""').drop_duplicates().rename(columns={"val": staging_col})

    # :v is the SQL dynamic variable
    insert_sql = text(f"""
        INSERT INTO {staging}.{staging_table} ({staging_col})
        VALUES (:v)
        ON CONFLICT ({staging_col}) DO NOTHING
    """)

    with engine.begin() as conn:
        # Clear tables
        conn.execute(text(f"TRUNCATE TABLE {staging}.{staging_table} RESTART IDENTITY CASCADE;"))
        # Insert values if exist
        if not df.empty:
            conn.execute(insert_sql, [{"v": v} for v in df[staging_col]])
        
    print(f"Loaded {len(df)} rows into {staging}.{staging_table}")


# Loading procedure (Necessary due to specificity)
def load_procedure():
    # Extract from raw
    df = pd.read_sql(text(f"""
        SELECT DISTINCT "CDT Code" AS cdt_code,
                        "Procedure Name" AS name,
                        "Service Type" AS service_type
        FROM {raw}."Procedure"
        WHERE "CDT Code" IS NOT NULL
    """), engine)

    # Clean values
    df["cdt_code"] = df["cdt_code"].str.strip()
    df["name"] = df["name"].str.strip() 
    df["service_type"] = df["service_type"].str.strip()
    df = df.drop_duplicates()

    # Resolve foreign key
    service_df = pd.read_sql(f"SELECT service_type_id, type FROM {staging}.service_type", engine)

    # Create joined table and drop unnecessary cols
    df = (
        df.merge(service_df, how="inner", left_on="service_type", right_on="type")
        .drop(columns=["service_type", "type"])
    )
    df["service_type_id"]= df["service_type_id"].astype("Int64")

    # Insert into staging
    insert_sql = text(f"""
        INSERT INTO {staging}.procedure (cdt_code, name, service_type_id)
        VALUES (:cdt_code, :name, :service_type_id)
        ON CONFLICT (cdt_code) DO NOTHING
    """)

    # Custom util function
    trunc_and_load(df, "procedure", insert_sql)
    
    print(f"Loaded {len(df)} rows into {staging}.procedure")

load_lookup("Carrier", "carrier", "Carrier Name", "name")
load_lookup("Network Type", "network_type", "Network Type", "name")
load_lookup("Plan Category", "plan_category", "Plan Category", "category")
load_lookup("Plan Type", "plan_type", "Plan Type", "name")
load_lookup("Service Type", "service_type", "Service Type", "type")
load_procedure()