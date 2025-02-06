import os
import logging
import asyncio
import asyncpg
import aiohttp
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
        self.nlsql_response = ""

        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "DB_HOST": os.getenv("PG_HOST", "XX.XX.XXX.XXX"),
                "DB_PORT": os.getenv("PG_PORT", 'XXXX'),
                "DB_USER": os.getenv("PG_USER", "admin"),
                "DB_PASSWORD": os.getenv("PG_PASSWORD", "XXXXX"),
                "DB_DATABASE": os.getenv("PG_DB", "XXXXX"),
                "DB_TABLES": ["XXXXX"],
            }
        )

    async def init_db_connection(self):
        try:
            conn = await asyncpg.connect(
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE,
                host=self.valves.DB_HOST,
                port=self.valves.DB_PORT
            )
            print("Connection to PostgreSQL established successfully")
            
            query = """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'pg_catalog');
            """
            
            tables = await conn.fetch(query)
            print("Tables in the database:")
            for row in tables:
                print(f"{row['table_schema']}.{row['table_name']}")
            
            await conn.close()
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")

    async def on_startup(self):
        await self.init_db_connection()

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
                await asyncio.sleep(2 ** attempt)

    async def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        keyword = user_message.lower().split("mostrar tablas que contengan")[-1].strip()

        try:
            conn = await asyncpg.connect(
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE,
                host=self.valves.DB_HOST,
                port=self.valves.DB_PORT
            )
            query = """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'pg_catalog')
                AND table_name ILIKE $1;
            """
            tables = await conn.fetch(query, f"%{keyword}%")
            await conn.close()

            if not tables:
                return f"No hay tablas que contenga la palabra: {keyword}"
            
            return str([f"{row['table_schema']}.{row['table_name']}" for row in tables])
        except Exception as e:
            logging.error(f"Error al obtener las tablas: {e}")
            return f"Error al obtener las tablas: {e}"
