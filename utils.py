from config import staging, engine
import pandas as pd
from sqlalchemy import text


def trunc_and_load(df, table, insert_sql):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {staging}.{table} RESTART IDENTITY CASCADE;"))
        if not df.empty:
            conn.execute(insert_sql, df.to_dict(orient="records"))

def drop_schema_tables(schema):
    with engine.begin() as conn:
        conn.execute(text(f"""
            DO $$
                DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = '{schema}')
                    LOOP
                        EXECUTE 'DROP TABLE {schema}.' || quote_ident(r.tablename) || ' CASCADE;';
                    END LOOP;
                END $$;
        """))
        
    print("Schema tables successfully dropped.")