import os
import logging
import psycopg2


from pydantic import BaseModel
from typing import List


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



    
