import os
import logging
import openai
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
        self.name = "Text to SQL"
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

        # Configurar OpenAI API
    openai.api_key = os.getenv("test")

    def generate_sql_query(self, user_message: str) -> str:
        """
        Usa OpenAI para convertir un mensaje en lenguaje natural en una consulta SQL válida.
        """
        prompt = (
            "Eres un generador de consultas SQL para PostgreSQL. "
            "Convierte la siguiente solicitud en una consulta SQL válida y segura. "
            "Por favor, genera SOLO una consulta SQL válida, sin ninguna explicación. "
            "Ejemplo:\n"
            "Entrada: 'Mostrar todos los usuarios activos'\n"
            "Salida: SELECT * FROM users WHERE status = 'active';\n\n"
            "Entrada: '" + user_message + "'\n"
            "Salida:"
        )

        try:
            # Llamada a la API con la nueva interfaz
            response = openai.chat.completions.create(
            model="gpt-3.5-turbo",  # Usa el modelo adecuado
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.7
        )
        
            
            generated_text = response["choices"][0]["message"]["content"].strip()
        
            # Verificación de la consulta SQL generada
            if not generated_text.lower().startswith("select"):
                return "Error: No se pudo generar una consulta SQL válida."
        
            return generated_text
     
        except openai.OpenAIError as e:
            return f"Error de OpenAI: {e}"


    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Toma un mensaje de usuario y lo convierte en una consulta SQL."""
        try:
            conn = psycopg2.connect(
                database=self.valves.DB_DATABASE,
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                host=self.valves.DB_HOST.split('//')[-1],
                port=self.valves.DB_PORT
                )
            # Generar la consulta SQL
            sql_query = self.generate_sql_query(user_message)
            print(f"Consulta generada: {sql_query}")
            
            # Devolver solo la consulta SQL sin ejecutarla
            return sql_query
        except Exception as e:
            logging.error(f"Error processing request: {e}")
            return f"Error: {e}"


