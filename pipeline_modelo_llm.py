import os
import logging
import psycopg2  # Biblioteca popular para interactuar con bases de datos PostgreSQL
import aiohttp
import asyncio
import ollama  # Necesitas la librería Ollama
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
        self.cur = None  # Añadimos el cursor como atributo
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

            # Crear un cursor y asignarlo como atributo
            self.cur = self.conn.cursor()

        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL: {e}")
            raise  # Lanza la excepción para que puedas capturarla fuera si es necesario

    async def on_startup(self):
        self.init_db_connection()

    async def on_shutdown(self):
        if self.cur:
            self.cur.close()
        if self.conn:
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
            # Si no tenemos la conexión, la inicializamos
            if not self.conn:
                self.init_db_connection()

            if not self.cur:  # Si el cursor no está creado, lo creamos
                self.cur = self.conn.cursor()

            # Consultar las tablas de la base de datos
            self.cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'pg_catalog')
                AND table_name ILIKE %s;
            """, (f"%{keyword}%",))

            # Obtener los resultados
            tables = self.cur.fetchall()

            # Si no hay tablas que coincidan con la palabra clave, devolver el mensaje apropiado
            if not tables:
                return f"No hay tablas que contengan la palabra: {keyword}"

            # Crear una lista de tablas
            table_list = [f"{schema}.{table}" for schema, table in tables]

            # Generar contexto para el modelo Llama
            context = f"Las tablas en la base de datos que contienen '{keyword}' son: {', '.join(table_list)}. ¿Cómo puedo ayudarte con estas tablas?"

            # Llamar a Llama a través de Ollama para obtener una respuesta
            try:
                response = ollama.chat(model="llama", messages=[{"role": "user", "content": context}])
                response_text = response['text']
            except Exception as e:
                logging.error(f"Error llamando a Ollama: {e}")
                response_text = "Error al procesar la solicitud con el modelo Llama."

            # Devolver la respuesta generada por Llama
            return response_text

        except Exception as e:
            logging.error(f"Error al obtener las tablas: {e}")
            return f"Error al obtener las tablas: {e}"
