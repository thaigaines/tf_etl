# psycopg2-binary
from config import DATABASE_URL
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(DATABASE_URL)

def drop_tables(schema):
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
        
    print("Schema tables sucessfully dropped.")

drop_tables('staging')