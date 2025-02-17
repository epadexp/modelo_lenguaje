import requests
import logging
import os
import logging
import psycopg2 # Biblioteca popular para interacturar con bases de datos PostgreSQL
import aiohttp
import asyncio

from typing import List, Union, Generator, Iterator
from pydantic import BaseModel


logging.basicConfig(level=logging.DEBUG)

class Pipeline:

    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str
        DB_DATABASE: str
        #DB_TABLES: List[str]

    def __init__(self):
        self.name = "Consulta a Base de Datos"
        self.conn = None
        self.nlsql_response = ""

        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "DB_HOST": os.getenv("PG_HOST", "http://XX.XX.XXX.XXX"),
                "DB_PORT": os.getenv("PG_PORT", 'XXXX'),
                "DB_USER": os.getenv("PG_USER", "XXXXX"),
                "DB_PASSWORD": os.getenv("PG_PASSWORD", "XXXXX"),
                "DB_DATABASE": os.getenv("PG_DB", "XXXXX"),
                #"DB_TABLES": ["XXXXX"],
            }
        )

        self.url = "http://host.docker.internal:11434/v1/chat/completions" # Api de Ollama
        self.headers = {
            "Content-Type": "application/json"
        }
        self.model = "llama3"  # Modelo que estás usando

    
    def generate_sql_query(self, user_message: str) -> str:
        
        prompt = (f"""
                Tu tarea es generar una consulta SQL para PostgreSQL. La consulta debe buscar tablas cuyo nombre contenga una palabra clave proporcionada por el usuario.

                **Reglas:**
                1. La consulta debe ser válida y segura, usando `ILIKE` para realizar una búsqueda flexible.
                2. No añadas explicaciones ni texto adicional.
                3. La consulta debe buscar el nombre de las tablas en la base de datos, filtrando solo por tablas que contengan la palabra clave dada.
                4. No repitas ni reescribas la solicitud del usuario; solo devuelve la consulta SQL correcta.
                5. No agregues explicaciones ni ```sql, solo devuelve la consulta.
                6. **Debes reemplazar la s en el ILIKE por la palabra clave directamente en la consulta, no dejes el parámetro s en la consulta.**

                **Ejemplo:**
                Entrada: "Busca las tablas relacionadas con nacimientos"
                Salida:
                ```sql
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'pg_catalog')
                AND table_name ILIKE '%nacimientos%';

                **Ejemplo:**
                Entrada: "Quiero las tablas relacionadas con platanos"
                Salida:
                ```sql
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'pg_catalog')
                AND table_name ILIKE '%platanos%';

                **Ejemplo:**
                Entrada: "Las tablas sobre parados"
                Salida:
                ```sql
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'pg_catalog')
                AND table_name ILIKE '%parados%';

                Entrada:"{user_message}"
                Salida:

            """)


        
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": prompt}],
            "temperature": 0.7
        }
        
        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            response.raise_for_status()  # Esto lanzará una excepción si la respuesta no es 2xx
            
            # Obtener los datos de la respuesta
            response_data = response.json()
            print("Respuesta completa:", response_data)
            
            # Acceder al contenido de la respuesta
            if 'choices' in response_data and len(response_data['choices']) > 0:
                sql_query = response_data['choices'][0]['message']['content'].strip()
                return sql_query
            else:
                logging.error("La respuesta no contiene contenido válido.")
                return "Error: La respuesta no contiene contenido válido."

        except requests.exceptions.RequestException as e:
            logging.error(f"Error al realizar la solicitud a la API de Ollama: {e}")
            return "Error al generar la consulta SQL."
        


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


    def generate_natural_language_response(self, query_result: list) -> str:
        prompt = (f"""
            Tu tarea es generar una respuesta coherente en lenguaje natural en español a partir de los resultados de una consulta SQL a una base de datos PostgreSQL.
            
            Los resultados de la consulta SQL son los siguientes:
            
            {query_result}
            
            **Reglas:**
            1. Proporciona una respuesta clara y natural que interprete los resultados de la consulta.
            2. Si los resultados son vacíos, indica que no se encontraron resultados.
            3. No agregues explicaciones adicionales ni detalles técnicos.
            4. La respuesta debe ser breve pero informativa.
            5. La respuesta debe ser en español.
            
            Entrada: "{query_result}"
            Salida:
        """)

        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": prompt}],
            "temperature": 0.7
        }

        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            response.raise_for_status()  # Esto lanzará una excepción si la respuesta no es 2xx
            
            response_data = response.json()
            print("Respuesta completa:", response_data)
            
            if 'choices' in response_data and len(response_data['choices']) > 0:
                natural_response = response_data['choices'][0]['message']['content'].strip()
                return natural_response
            else:
                logging.error("La respuesta no contiene contenido válido.")
                return "Error: La respuesta no contiene contenido válido."

        except requests.exceptions.RequestException as e:
            logging.error(f"Error al realizar la solicitud a la API de Ollama: {e}")
            return "Error al generar la respuesta en lenguaje natural."


    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        
        # Extraer la palabra clave de la consulta del usuario
        # keyword = user_message.lower().split("mostrar tablas que contengan")[-1].strip()
  
        try:
                sql_query = self.generate_sql_query(user_message)
                
                
                # Establecer la conexión con la base de datos
                conn = psycopg2.connect(
                    database=self.valves.DB_DATABASE,
                    user=self.valves.DB_USER,
                    password=self.valves.DB_PASSWORD,
                    host=self.valves.DB_HOST.split('//')[-1],
                    port=self.valves.DB_PORT
                )
                conn.autocommit = True
                cursor = conn.cursor()

                # Consultar las tablas de la base de datos
                cursor.execute(sql_query)
                
                # Obtener los resultados
                tables= cursor.fetchall()

                

                # Si no hay tablas que coincidan con la palabra clave, devolver el mensaje apropiado
                if not tables:
                    return f"No hay tablas para lo que pides"
                
                respuesta=self.generate_natural_language_response(tables)

                # Crear una lista de tablas
                # table_list = [f"{schema}.{table}" for schema, table in tables]

                # Cerrar la conexión
                cursor.close()
                conn.close()

                # Devolver la lista de tablas como una cadena
                return str(respuesta)

        except Exception as e:
                logging.error(f"Error al obtener las tablas: {e}")
                return f"Error al obtener las tablas: {e}"
