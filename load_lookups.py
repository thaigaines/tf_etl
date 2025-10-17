# psycopg2-binary
from config import DATABASE_URL
import pandas as pd
from sqlalchemy import create_engine, text


engine = create_engine(DATABASE_URL)

raw = "raw"
staging = "staging"

def load_lookup(raw_table, staging_table, raw_col, staging_col):
    q = text(f'SELECT DISTINCT "{raw_col}" AS val FROM {raw}."{raw_table}" WHERE "{raw_col}" IS NOT NULL')
    df = (
        # Cleaning column values
        pd.read_sql(q, engine)
          .assign(val=lambda d: d["val"].str.strip())
          .query('val != ""')
          .drop_duplicates()
          .rename(columns={"val": staging_col})
    )
    print("Created df")
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
    df = df.assign(
        # .assign parameter: column_name=function
        # i.e. it passes back a function on columns of "d" that run .strip().
        # lambda creates an anonymous inline function after :
        cdt_code=lambda d: d["cdt_code"].str.strip(),
        name=lambda d: d["name"].str.strip(),
        service_type=lambda d: d["service_type"].str.strip()
    ).drop_duplicates()

    # Resolve foreign key
    service_df = pd.read_sql(f"SELECT service_type_id, type FROM {staging}.service_type", engine)

    # Create joined table and drop unnecessary cols
    df = (
    df.merge(service_df, how="inner", left_on="service_type", right_on="type")
      .drop(columns=["service_type", "type"])
      .assign(service_type_id=lambda d: d["service_type_id"].astype("Int64"))
    )

    # Insert into staging
    insert_sql = text(f"""
        INSERT INTO {staging}.procedure (cdt_code, name, service_type_id)
        VALUES (:cdt_code, :name, :service_type_id)
        ON CONFLICT (cdt_code) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {staging}.procedure RESTART IDENTITY CASCADE;"))
        if not df.empty:
            conn.execute(insert_sql, df.to_dict(orient="records"))

    print(f"Loaded {len(df)} rows into {staging}.procedure")

load_lookup("Carrier", "carrier", "Carrier Name", "name")
load_lookup("Network Type", "network_type", "Network Type", "name")
load_lookup("Plan Category", "plan_category", "Plan Category", "category")
load_lookup("Plan Type", "plan_type", "Plan Type", "name")
load_lookup("Service Type", "service_type", "Service Type", "type")
load_procedure()