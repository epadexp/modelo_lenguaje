import os
import logging
import psycopg2 # Biblioteca popular para interacturar con bases de datos PostgreSQL
import aiohttp
import asyncio


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
        self.name = "Lista Tablas Pipeline"
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

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        
        # Extraer la palabra clave de la consulta del usuario
        keyword = user_message.lower().split("mostrar tablas que contengan")[-1].strip()
  
        try:
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
                cursor.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                    AND table_schema NOT IN ('information_schema', 'pg_catalog')
                    AND table_name ILIKE %s;
            """, (f"%{keyword}%",))

                # Obtener los resultados
                tables = cursor.fetchall()

                # Si no hay tablas que coincidan con la palabra clave, devolver el mensaje apropiado
                if not tables:
                    return f"No hay tablas que contenga la palabra: {keyword}"

                # Crear una lista de tablas
                table_list = [f"{schema}.{table}" for schema, table in tables]

                # Cerrar la conexión
                cursor.close()
                conn.close()

                # Devolver la lista de tablas como una cadena
                return str(table_list)

        except Exception as e:
                logging.error(f"Error al obtener las tablas: {e}")
                return f"Error al obtener las tablas: {e}"
