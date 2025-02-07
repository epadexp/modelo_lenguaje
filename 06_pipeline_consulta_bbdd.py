import requests
import logging
import os
import psycopg2
import aiohttp
import asyncio
from typing import List, Union, Generator, Iterator
from pydantic import BaseModel
import json

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str
        DB_DATABASE: str

    def __init__(self):
        self.name = "Consulta a Base de Datos"
        self.conn = None
        self.nlsql_response = ""

        self.valves = self.Valves(
            **{
                "DB_HOST": os.getenv("PG_HOST", "http://XX.XX.XXX.XXX"),
                "DB_PORT": os.getenv("PG_PORT", 'XXXX'),
                "DB_USER": os.getenv("PG_USER", "XXXXX"),
                "DB_PASSWORD": os.getenv("PG_PASSWORD", "XXXXX"),
                "DB_DATABASE": os.getenv("PG_DB", "XXXXX"),
            }
        )

        self.url = "http://host.docker.internal:11434/v1/chat/completions"  # API de Ollama
        self.headers = {"Content-Type": "application/json"}
        self.model = "llama3"  # Modelo que estás usando

    def get_db_schema(self):
        """Obtiene la estructura de la base de datos (tablas y columnas)."""
        schema_info = {}

        try:
            conn = psycopg2.connect(
                database=self.valves.DB_DATABASE,
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                host=self.valves.DB_HOST.split('//')[-1],
                port=self.valves.DB_PORT
            )
            cursor = conn.cursor()

            # Obtener todas las tablas
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'pg_catalog');
            """)
            tables = [row[0] for row in cursor.fetchall()]

            for table in tables:
                # Obtener columnas de cada tabla
                cursor.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table}';
                """)
                schema_info[table] = [{"column": row[0], "type": row[1]} for row in cursor.fetchall()]

            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error al obtener la estructura de la base de datos: {e}")
            return {}

        return schema_info

    def generate_sql_query(self, user_message: str) -> str:
        """Genera una consulta SQL basada en una pregunta en lenguaje natural."""
        
        # Obtener la estructura de la base de datos
        db_schema = self.get_db_schema()
        
        prompt = f"""
        Eres un asistente experto en bases de datos PostgreSQL. Tu tarea es generar una consulta SQL 
        válida en base a una pregunta en lenguaje natural. La base de datos contiene las siguientes tablas y columnas:

        {json.dumps(db_schema, indent=2)}

        **Reglas:**
        1. Devuelve solo la consulta SQL sin explicaciones adicionales.
        2. Asegúrate de que la consulta sea válida en PostgreSQL.
        3. Usa alias y joins si es necesario para unir varias tablas.
        4. Si la pregunta no se puede responder con la base de datos proporcionada, devuelve un mensaje de error.

        **Pregunta del usuario:**
        "{user_message}"

        **Salida esperada:**
        """

        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": prompt}],
            "temperature": 0.5
        }

        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            response.raise_for_status()

            response_data = response.json()
            if 'choices' in response_data and len(response_data['choices']) > 0:
                sql_query = response_data['choices'][0]['message']['content'].strip()
                return sql_query
            else:
                logging.error("La respuesta no contiene contenido válido.")
                return "Error: La respuesta no contiene contenido válido."

        except requests.exceptions.RequestException as e:
            logging.error(f"Error al realizar la solicitud a la API de Ollama: {e}")
            return "Error al generar la consulta SQL."

    def execute_query(self, sql_query: str):
        """Ejecuta la consulta SQL en la base de datos y devuelve los resultados."""
        try:
            conn = psycopg2.connect(
                database=self.valves.DB_DATABASE,
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                host=self.valves.DB_HOST.split('//')[-1],
                port=self.valves.DB_PORT
            )
            cursor = conn.cursor()
            cursor.execute(sql_query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return results

        except Exception as e:
            logging.error(f"Error al ejecutar la consulta SQL: {e}")
            return []

    def generate_natural_language_response(self, query_result: list) -> str:
        """Convierte los resultados de la consulta en una respuesta en lenguaje natural."""
        prompt = f"""
        Eres un asistente que transforma resultados de consultas SQL en respuestas en lenguaje natural en español.
        
        **Resultados de la consulta:**
        {query_result}

        **Reglas:**
        1. Si la consulta no devuelve resultados, responde "No se encontraron datos".
        2. Si hay resultados, resume la información en una respuesta clara y concisa.
        3. La respuesta debe estar en español.
        
        **Salida esperada:**
        """

        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": prompt}],
            "temperature": 0.7
        }

        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            response.raise_for_status()
            response_data = response.json()

            if 'choices' in response_data and len(response_data['choices']) > 0:
                return response_data['choices'][0]['message']['content'].strip()
            else:
                return "Error: La respuesta no contiene contenido válido."

        except requests.exceptions.RequestException as e:
            logging.error(f"Error al generar la respuesta en lenguaje natural: {e}")
            return "Error al generar la respuesta."

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Orquesta la generación y ejecución de la consulta SQL."""
        try:
            sql_query = self.generate_sql_query(user_message)
            if "Error" in sql_query:
                return sql_query

            query_results = self.execute_query(sql_query)
            natural_response = self.generate_natural_language_response(query_results)
            return natural_response

        except Exception as e:
            logging.error(f"Error en el proceso de consulta: {e}")
            return f"Error en el proceso: {e}"

