import subprocess
import logging
import psycopg2
import os
import json
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
        self.name = "Modelo LLM Pipeline"
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

    def query_from_llm(self, user_message: str) -> str:
        """
        Usa Llama (Ollama) para convertir una pregunta del usuario en una consulta SQL.
        """
        try:
            # Ejecutamos el comando de Ollama para obtener la consulta SQL
            result = subprocess.run(
                ["ollama", "run", "llama2", "--text", user_message],
                capture_output=True, text=True, check=True
            )
            # Procesar la respuesta JSON de Ollama
            response = json.loads(result.stdout)
            sql_query = response.get("text", "")
            return sql_query
        except Exception as e:
            logging.error(f"Error al generar la consulta SQL con Llama: {e}")
            return "Error al generar la consulta SQL."

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """
        Procesa el mensaje del usuario, convierte la pregunta en una consulta SQL y ejecuta la consulta.
        """
        # Usar el LLM (Llama) para obtener la consulta SQL
        sql_query = self.query_from_llm(user_message)

        if sql_query:
            try:
                # Conectar a la base de datos
                conn = psycopg2.connect(
                    database=self.valves.DB_DATABASE,
                    user=self.valves.DB_USER,
                    password=self.valves.DB_PASSWORD,
                    host=self.valves.DB_HOST.split('//')[-1],
                    port=self.valves.DB_PORT
                )
                conn.autocommit = True
                cursor = conn.cursor()

                # Ejecutar la consulta SQL generada por Llama
                cursor.execute(sql_query)

                # Obtener los resultados
                results = cursor.fetchall()

                # Si no hay resultados, devolver mensaje apropiado
                if not results:
                    return "No se encontraron resultados."

                # Formatear los resultados para devolverlos como una cadena
                result_str = "\n".join([str(result) for result in results])

                # Cerrar la conexión
                cursor.close()
                conn.close()

                return result_str

            except Exception as e:
                logging.error(f"Error al ejecutar la consulta SQL: {e}")
                return f"Error al ejecutar la consulta SQL: {e}"
