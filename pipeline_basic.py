import os
import logging
import psycopg2


from pydantic import BaseModel
from typing import List, Union, Generator, Iterator



logging.basicConfig(level=logging.DEBUG)

class Pipeline:

    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str
        DB_DATABASE: str
        DB_TABLES: List[str]

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
            }
        )

        logging.info(f"Pipeline '{self.name}' initialized with configuration: {self.valves.dict()}")


    # def connect_to_db(self):
    #     connection_params = {
    #         'dbname': self.valves.DB_DATABASE,
    #         'user': self.valves.DB_USER,
    #         'password': self.valves.DB_PASSWORD,
    #         'host': self.valves.DB_HOST.split('//')[-1],  # Remove the http:// or https:// prefix if present
    #         'port': self.valves.DB_PORT
    #     }

    #       # Intentar establecer la conexión
    #     try:
    #         self.conn = psycopg2.connect(**connection_params)
    #         self.conn.set_client_encoding('UTF8')  # Establecer explícitamente la codificación a UTF8
    #         print("Connection to PostgreSQL established successfully")
    #     except Exception as e:
    #         print(f"Error connecting to PostgreSQL: {e}")

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # Use the established psycopg2 connection to create a SQLAlchemy engine
        #self.engine = create_engine(f"postgresql+psycopg2://{self.valves.DB_USER}:{self.valves.DB_PASSWORD}@{self.valves.DB_HOST.split('//')[-1]}:{self.valves.DB_PORT}/{self.valves.DB_DATABASE}")
        
        
        try:
            conn = psycopg2.connect(
                database=self.valves.DB_DATABASE,
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                host=self.valves.DB_HOST.split('//')[-1],
                port=self.valves.DB_PORT
                )

            return "Conexión correcta"

        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")

        



    




    
