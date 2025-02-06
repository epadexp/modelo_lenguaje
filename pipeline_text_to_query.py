import os
import logging
import psycopg2
from transformers import AutoModelForCausalLM, AutoTokenizer

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

        # Cargar el modelo y el tokenizador de LLaMA
        self.tokenizer = AutoTokenizer.from_pretrained("huggingface/llama3")  # Reemplaza con el path o nombre del modelo correcto
        self.model = AutoModelForCausalLM.from_pretrained("huggingface/llama3")  # Reemplaza con el path o nombre del modelo correcto

    def generate_sql_query(self, user_message: str) -> str:
        """
        Usa LLaMA para convertir un mensaje en lenguaje natural en una consulta SQL válida.
        """
        prompt = (
            "Eres un generador de consultas SQL para PostgreSQL. "
            "Convierte la siguiente solicitud en una consulta SQL válida y segura. "
            "No agregues explicaciones, solo devuelve la consulta SQL.\n\n"
            "Ejemplo:\n"
            "Entrada: 'Mostrar todos los usuarios activos'\n"
            "Salida: SELECT * FROM users WHERE status = 'active';\n\n"
            "Entrada: '" + user_message + "'\n"
            "Salida:"
        )

        # Tokenizar el prompt
        inputs = self.tokenizer(prompt, return_tensors="pt")

        # Generar la respuesta con el modelo LLaMA
        outputs = self.model.generate(**inputs, max_length=512)

        # Decodificar la respuesta generada
        sql_query = self.tokenizer.decode(outputs[0], skip_special_tokens=True).strip()

        # Asegurar que la respuesta sea una consulta SQL válida
        if not sql_query.lower().startswith("select"):
            logging.error("El modelo no generó una consulta SQL válida.")
            return "Error: No se pudo generar una consulta SQL válida."
        
        return sql_query

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


