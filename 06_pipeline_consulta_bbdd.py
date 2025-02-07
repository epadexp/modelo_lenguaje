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
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_name, ordinal_position;
            """)
                # Organizar los datos en un diccionario {tabla: [columnas]}
            schema = {}
            for table, column in cursor.fetchall():
                if table not in schema:
                    schema[table] = []
                schema[table].append(column)

            cursor.close()
            conn.close()

            return schema

        except psycopg2.Error as e:
            logging.error(f"Error al obtener la estructura de la base de datos: {e}")
            return {}

    def generate_sql_query(self, user_message: str) -> str:
        """Genera una consulta SQL basada en una pregunta en lenguaje natural."""
        
        # Obtener la estructura de la base de datos
        db_schema = self.get_db_schema()

        if not db_schema:
            return "Error: No se pudo obtener la estructura de la base de datos."

        prompt = f"""
        Eres un asistente experto en bases de datos PostgreSQL. Tu tarea es generar una consulta SQL válida
        usando exclusivamente las siguientes tablas y columnas disponibles en la base de datos:

        {json.dumps(db_schema, indent=2)}

        **Reglas:**
        1. Devuelve solo la consulta SQL sin explicaciones adicionales ni comentarios.
        2. La consulta debe ser válida en PostgreSQL y solo puede usar tablas y columnas listadas arriba.
        3. Usa `JOIN` si la información se encuentra en múltiples tablas.
        4. No inventes nombres de tablas o columnas. Usa solo las que existen.
        5. No devuelvas texto descriptivo, solo la consulta SQL.
        6. Si el usuario especifica ine, tienes que buscar entre las tablas que empiezan por ine
        7. Si el usuario especifica istac, tienes que buscar entre las tablas que empiezan por istac
        8. Si el usuario especifica un año, tienes que buscar en la columna periodo

        **Ejemplos de entrada y salida:**

        Entrada: "¿Cuántos nacimientos hubieron en Aragón en 2023 según el ine?"
        Salida:
        SELECT COUNT(*) FROM ine_1_3_nacimientos;

        Entrada: "¿Cuántos nacimientos de hombres hubieron en 2023 según el ine?"
        Salida:
        SELECT valor
        FROM ine_1_3_nacimientos 
        WHERE sexo = 'hombres' AND periodo = 2023;

        Entrada del usuario:
        "{user_message}"

        Salida esperada (solo consulta SQL válida):
        """

        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": prompt}],
            "temperature": 0.3
        }

        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            response.raise_for_status()

            response_data = response.json()
            if 'choices' in response_data and len(response_data['choices']) > 0:
                sql_query = response_data['choices'][0]['message']['content'].strip()

                # Validar que la respuesta es SQL válido
                if not sql_query.lower().startswith(("select", "insert", "update", "delete", "with")):
                    logging.error(f"Error: El modelo no devolvió SQL válido: {sql_query}")
                    return "Error: El modelo no devolvió una consulta SQL válida."

                return sql_query
            else:
                logging.error("La respuesta no contiene contenido válido.")
                return "Error: La respuesta no contiene contenido válido."

        except requests.exceptions.RequestException as e:
            logging.error(f"Error al realizar la solicitud a la API de Ollama: {e}")
            return "Error al generar la consulta SQL."



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
        
    def execute_query(self, sql_query: str):
        """Ejecuta una consulta SQL en la base de datos PostgreSQL y devuelve los resultados."""
        try:
            # Establecer conexión con la base de datos
            conn = psycopg2.connect(
                database=self.valves.DB_DATABASE,
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                host=self.valves.DB_HOST.split('//')[-1],
                port=self.valves.DB_PORT
            )
            cursor = conn.cursor()

            # Ejecutar la consulta
            cursor.execute(sql_query)

            # Obtener los resultados
            results = cursor.fetchall()

            # Cerrar conexión
            cursor.close()
            conn.close()

            return results

        except psycopg2.Error as e:
            logging.error(f"Error al ejecutar la consulta SQL: {e}")
            return f"Error en la ejecución de la consulta SQL: {e}"


    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        try:
            sql_query = self.generate_sql_query(user_message)

            # Validar que se generó una consulta SQL válida
            if sql_query.startswith("Error"):
                return sql_query

            # Ejecutar la consulta en PostgreSQL
            results = self.execute_query(sql_query)

            # Si no hay resultados, devolver un mensaje apropiado
            if not results:
                return "No hay resultados para tu consulta."

            # Convertir los resultados en una respuesta en lenguaje natural
            respuesta = self.generate_natural_language_response(results)

            return respuesta

        except Exception as e:
            logging.error(f"Error en el proceso: {e}")
            return f"Error en el proceso: {e}"


