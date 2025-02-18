import pandas as pd
import sqlalchemy as sa

import contextlib

#function that will flexibly handle inserts to the tables in the database
#create temp table in the db, left join to final table, insert data that does not exist
#params:    destination database, schema, table
#           unique columns to left join
#           destination column list
#           dataframe of data to be inserted
#           match dataframe columns to destination columns

def tt_write_to_db(database_name: str, schema: str, table: str, pk_col_list: list, pk_skip, df_to_insert: pd.DataFrame):
    dest_columns = get_col_definitions(database_name, schema, table)
    dest_column_list = list(dest_columns['field_name'])
    df_to_insert.columns = dest_column_list
    first_col = pk_col_list[0]
    print(df_to_insert)
    insert_col_list = ', '.join([f'"{field_name}"' for field_name in df_to_insert])

    select_col_list = ', '.join([f'tmp."{field_name}"' for field_name in df_to_insert])

    stmt = f"INSERT INTO {table} ({insert_col_list})\n"
    stmt += f"SELECT DISTINCT {select_col_list} FROM {schema}.{table}_temp tmp \n"
    stmt += f"LEFT JOIN {table}  dst ON \n"
    stmt += " AND ".join(
        [f'tmp."{col}" = dst."{col}"' for col in pk_col_list]
    )
    stmt += f"\nWHERE (dst.{first_col} IS NULL AND tmp.{first_col} IS NOT NULL) \n"

    engine = login_postgres()

    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {schema}.{table}_temp")
        conn.exec_driver_sql(
            f"CREATE TABLE {table}_temp AS "
            f"SELECT distinct {insert_col_list}"
            f" FROM {table} WHERE false"
        )
        df_to_insert.to_sql(f"{table}_temp", conn, if_exists="append", index=False)
        conn.exec_driver_sql(stmt)


def get_col_definitions (database:str, schema:str, table:str) -> pd.DataFrame:
    query = f'select * from {schema}.fn_get_table_details(\'{schema}\', \'{table}\')'.format(sch=schema, tbl=table)
    engine = login_postgres()
    connectivity(engine)
    table_results = pd.read_sql_query(query, engine)

    return table_results

def get_primary_keys (database:str, schema:str, table:str) -> list:
    query = f'select * from {database}.{schema}.fn_get_pk_details(\'{schema}\', \'{table}\')'.format(db = database, sch=schema, tbl=table)
    engine = login_postgres()
    connectivity(engine)
    pk_results = pd.read_sql_query(query, engine)

    return pk_results

def login_postgres():
    engine = sa.create_engine('postgresql://postgres:BestPasswordEver@34.56.191.19:5432/postgres')
    return engine

def connectivity(engine):
    connection = None

    @contextlib.contextmanager
    def connect():
        nonlocal  connection

        if connection is None:
            connection = engine.connect()
            with connection:
                with connection.begin():
                    yield connection
        else:
            yield connection

    return connect

