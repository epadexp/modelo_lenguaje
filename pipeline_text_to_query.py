import os
import logging
import psycopg2
import ollama  # Para usar el modelo de lenguaje LLaMA con Ollama

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
        self.name = "Basic Pipeline"
        self.conn = None
        self.nlsql_response = ""

        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "DB_HOST": os.getenv("PG_HOST", "http://XX.XX.XXX.XXX"),
                "DB_PORT": os.getenv("PG_PORT", 'XXXX'),
                "DB_USER": os.getenv("PG_USER", "admin"),
                "DB_PASSWORD": os.getenv("PG_PASSWORD", "XXXXX"),
                "DB_DATABASE": os.getenv("PG_DB", "XXXXX"),
                "DB_TABLES": ["XXXXX"],
            }
        )
    
    def generate_sql_query(self, user_message: str) -> str:
        """
        Usa el modelo de lenguaje para convertir un mensaje en lenguaje natural en una consulta SQL.
        """
        prompt = f"Convierte el siguiente mensaje en una consulta SQL para PostgreSQL:\n\nMensaje: '{user_message}'\n\nConsulta SQL:"
        response = ollama.chat(model='llama3', messages=[{"role": "user", "content": prompt}])
        return response["message"]["content"].strip()

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Toma un mensaje de usuario y lo convierte en una consulta SQL."""
        try:
            # Generar la consulta SQL
            sql_query = self.generate_sql_query(user_message)
            print(f"Consulta generada: {sql_query}")
            
            # Devolver solo la consulta SQL sin ejecutarla
            return sql_query
        except Exception as e:
            logging.error(f"Error processing request: {e}")
            return f"Error: {e}"
