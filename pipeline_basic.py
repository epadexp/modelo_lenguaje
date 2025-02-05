import os
import logging
import psycopg2
import aiohttp
import asyncio



from pydantic import BaseModel
from typing import List, Union, Generator, Iterator
from llama_index.core import SQLDatabase
from llama_index.llms.openai_like import OpenAILike
from sqlalchemy import create_engine


logging.basicConfig(level=logging.DEBUG)

class Pipeline:

    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str
        DB_DATABASE: str
        DB_TABLES: List[str]
        VLLM_HOST: str
        OPENAI_API_KEY: str
        TEXT_TO_SQL_MODEL: str

    def __init__(self):
        self.name = "01 Database RAG Pipeline vLLM llama"
        self.conn = None
        self.nlsql_response = ""

        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "DB_HOST": os.getenv("PG_HOST", "http://10.30.164.243"),
                "DB_PORT": os.getenv("PG_PORT", '5432'),
                "DB_USER": os.getenv("PG_USER", "admin"),
                "DB_PASSWORD": os.getenv("PG_PASSWORD", "tester123"),
                "DB_DATABASE": os.getenv("PG_DB", "postgres"),
                "DB_TABLES": ["movies"],
                "VLLM_HOST": os.getenv("VLLM_HOST", "http://10.1.152.55:8012/v1"),
                "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", 'abc-123'),
                "TEXT_TO_SQL_MODEL": "NousResearch/Meta-Llama-3-8B-Instruct"
            }
        )


    def init_db_connection(self):
        connection_params = {
            'dbname': self.valves.DB_DATABASE,
            'user': self.valves.DB_USER,
            'password': self.valves.DB_PASSWORD,
            'host': self.valves.DB_HOST.split('//')[-1],  # Remove the http:// or https:// prefix if present
            'port': self.valves.DB_PORT
        }
    
        try:
            self.conn = psycopg2.connect(**connection_params)
            print("Connection to PostgreSQL established successfully")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")


        # Create a cursor object
        self.cur = self.conn.cursor()

        # Query to get the list of tables
        self.cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN ('information_schema', 'pg_catalog');
        """)

        # Fetch and print the table names
        tables = self.cur.fetchall()
        print("Tables in the database:")
        for schema, table in tables:
            print(f"{schema}.{table}")
        
        
        # Query to get the column names
        self.cur.execute("""SELECT json_object_keys(to_json(json_populate_record(NULL::public.movies, '{}'::JSON)))""")
        
        #Fetch and print the column names
        columns = self.cur.fetchall()
        print("Columns in the database:")
        print(f"{columns}")




    async def on_startup(self):
        self.init_db_connection()

    async def on_shutdown(self):
        self.cur.close()
        self.conn.close()

    async def make_request_with_retry(self, url, params, retries=3, timeout=10):
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=timeout) as response:
                        response.raise_for_status()
                        return await response.text()
            except (aiohttp.ClientResponseError, aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                logging.error(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt + 1 == retries:
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

    def extract_sql_query(self, response_object):
        for key, value in response_object.items():
            if isinstance(value, dict) and 'sql_query' in value:
                return value['sql_query']
            elif key == 'sql_query':
                return value
        return None

    def handle_streaming_response(self, response_gen):
        final_response = ""
        for chunk in response_gen:
            final_response += chunk
        return final_response

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # Use the established psycopg2 connection to create a SQLAlchemy engine
        self.engine = create_engine(f"postgresql+psycopg2://{self.valves.DB_USER}:{self.valves.DB_PASSWORD}@{self.valves.DB_HOST.split('//')[-1]}:{self.valves.DB_PORT}/{self.valves.DB_DATABASE}")
        sql_database = SQLDatabase(self.engine, include_tables=self.valves.DB_TABLES)



    




    
