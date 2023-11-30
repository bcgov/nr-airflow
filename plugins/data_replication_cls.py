import psycopg2
import psycopg2.pool
import psycopg2.extras
from psycopg2.extras import execute_batch
import configparser
import time
import json
import concurrent.futures
from datetime import datetime
import sys
import os
import argparse
import oracledb as oracledb 
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection

class data_replication(BaseOperator):
    def __init__(self, mstr_schema:str, app_name:str, env:str,concurrent_tasks =5, **kwargs)-> None:
        super().__init__(**kwargs)
        self.mstr_schema = mstr_schema 
        self.app_name = app_name
        self.env = env
        self.concurrent_tasks = concurrent_tasks
        self.OrcPool = None
        self.PgresPool = None

    @staticmethod
    def output_type_handler(cursor, metadata):
        def out_converter(d):
            if isinstance(d, str):
                return ' '.join(d.split('\x00'))
            else:
                return d

        if metadata.type_code is oracledb.DB_TYPE_NUMBER:
            return cursor.var(oracledb.DB_TYPE_VARCHAR,
                 arraysize=cursor.arraysize, outconverter=out_converter)

    def get_active_tables(self, mstr_schema,app_name, PgresPool):
        postgres_connection  = PgresPool.getconn()  
        postgres_cursor = postgres_connection.cursor()
        list_sql = f"""
        SELECT application_name,source_schema_name,source_table_name,target_schema_name,target_table_name,truncate_flag,cdc_flag,full_inc_flag,cdc_column,replication_order,where_clause
        from {mstr_schema}.cdc_master_table_list c
        where  active_ind = 'Y' and lower(application_name)='{app_name}'
        order by replication_order, source_table_name
        """
        print(list_sql)
        with postgres_connection.cursor() as curs:
                    curs.execute(list_sql)
                    rows = curs.fetchall()
        postgres_connection.commit()
        postgres_cursor.close()
        PgresPool.putconn(postgres_connection)
        return rows
       
    def get_connection_pools(self):       
        orcConn = Connection.get_connection_from_secrets(f'oracle_{self.app_name}_{self.env}_conn')
        pgresConn = Connection.get_connection_from_secrets(f'postgres_ods_{self.env}_conn')

        # In[3]: Retrieve Oracle database configuration
        oracle_username = orcConn.login
        oracle_password = orcConn.password
        oracle_host = orcConn.host
        oracle_port = orcConn.port
        oracle_database = orcConn.schema
        # In[4]: Retrieve Postgres database configuration
        postgres_username = pgresConn.login
        postgres_password = pgresConn.password
        postgres_host = pgresConn.host
        postgres_port = pgresConn.port
        postgres_database = pgresConn.schema

        dsn = f"{oracle_host}/{oracle_database}"
        OrcPool = oracledb.SessionPool(user=oracle_username, password=oracle_password, dsn=dsn, min=self.concurrent_tasks,
                                     max=self.concurrent_tasks, increment=1, encoding="UTF-8")
        PgresPool = psycopg2.pool.ThreadedConnectionPool(minconn = self.concurrent_tasks, maxconn = self.concurrent_tasks,host=postgres_host, port=postgres_port, dbname=postgres_database, user=postgres_username, password=postgres_password)
        
        return OrcPool, PgresPool

    def extract_from_oracle(self,OrcPool,table_name,source_schema,where_clause):
        sql_query = f'SELECT * FROM {source_schema}.{table_name}' + where_clause
        print(sql_query)
        # Acquire a connection from the pool
        oracle_connection = OrcPool.acquire()
        print("Connection acquired")
        oracle_cursor = oracle_connection.cursor()  
        #oracle_cursor.outputtypehandler = data_replication.output_type_handler
        
        try:
            # Use placeholders in the query and bind the table name as a parameter
            oracle_cursor.execute(sql_query)
            #num_rows = oracle_cursor.rowcount
            #print(f" num rows = {num_rows})
            rows = oracle_cursor.fetchall()
            
            print(f"Source row count = {len(rows)}")
            OrcPool.release(oracle_connection)
            return rows
        except Exception as e:
            print(f"Error extracting data from Oracle: {str(e)}")
            OrcPool.release(oracle_connection)
            return []
    def kill_pools(self):
        self.OrcPool.close()
        self.PgresPool.closeall()

    def load_into_postgres(self, PgresPool,table_name, data,target_schema):
        postgres_connection = PgresPool.getconn()
        postgres_cursor = postgres_connection.cursor()
        try:
            # Delete existing data in the target table
            delete_query = f'TRUNCATE TABLE {target_schema}.{table_name}'
            postgres_cursor.execute(delete_query)

            # Build the INSERT query with placeholders
            insert_query = f'INSERT INTO {target_schema}.{table_name} VALUES ({", ".join(["%s"] * len(data[0]))})'
            #insert_query = f'INSERT INTO {target_schema}.{table_name} VALUES %s'

            # Use execute_batch for efficient batch insert
            with postgres_connection.cursor() as cursor:
                # Prepare the data as a list of tuples
                data_to_insert = [(tuple(row)) for row in data]
                newestdata = []
                '''for row in data:
                    newdata = []
                    for x in tuple(row):
                        newdata.append(x.replace('\x00', '\uFFFD') if type(x)=='str' else x)
                    newestdata.append(tuple(newdata))
                '''
                execute_batch(cursor, insert_query, data_to_insert)
                postgres_connection.commit()
                print(f"Target: Data loaded into table: {table_name}")
        except Exception as e:
            print(f"Error loading data into PostgreSQL: {str(e)}")
        finally:
            # Return the connection to the pool
            if postgres_connection:
                postgres_cursor.close()
                PgresPool.putconn(postgres_connection)
            
    def start_replication(self):
        print('Starting Replication with below params..')
        print(f'Master Schema : {self.mstr_schema}, App Name: {self.app_name}, Env: {self.env}')
        self.OrcPool, self.PgresPool = self.get_connection_pools()
        active_tables_rows =self.get_active_tables(self.mstr_schema, self.app_name, self.PgresPool)
        tables_to_extract = [(row[2],row[1],row[3],row[10]) for row in active_tables_rows]
        for table in tables_to_extract:
            (table_name,source_schema,target_schema,where_clause) = (table[0],table[1],table[2],table[3])
            oracle_data = self.extract_from_oracle(self.OrcPool,table_name,source_schema,where_clause)       
            self.load_into_postgres(self.PgresPool,table_name, oracle_data, target_schema)
        self.kill_pools()

    def execute(self, context)-> None:
        self.start_replication()    
