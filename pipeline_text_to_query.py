import os
import logging
import openai
import psycopg2
from pydantic import BaseModel
from typing import List, Union, Generator, Iterator

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Valves(BaseModel):
        OPENAI_API_BASE_URL: str = "https://api.openai.com/v1"
        OPENAI_API_KEY: str = ""
        OPENAI_API_MODEL: str = "gpt-4o"
        OPENAI_API_TEMPERATURE: float = 0.7
        AGENT_SYSTEM_PROMPT: str = (
            "Eres un generador de consultas SQL para PostgreSQL. "
            "Convierte la siguiente solicitud en una consulta SQL válida y segura. "
            "No agregues explicaciones, solo devuelve la consulta SQL.\n\n"
            "Ejemplo:\n"
            "Entrada: 'Mostrar todos los usuarios activos'\n"
            "Salida: SELECT * FROM users WHERE status = 'active';\n\n"
            "Entrada: '  user_message  '\n"
            "Salida:"
        )

    def __init__(self):
        self.name = "Chat with YouTube"
        self.tools = None
        self.valves = self.Valves(
            OPENAI_API_KEY=os.getenv("OPENAI_API_KEY", "")
        )
        openai.api_key = self.valves.OPENAI_API_KEY



    def generate_sql_query(self, user_message: str) -> str:
        prompt = self.valves.AGENT_SYSTEM_PROMPT.format(user_message=user_message)
        
        try:
            response = openai.Completion.create(
                model=self.valves.OPENAI_API_MODEL,
                prompt=prompt,
                max_tokens=150,
                temperature=self.valves.OPENAI_API_TEMPERATURE,
            )
            sql_query = response.choices[0].text.strip()
            return sql_query
        except Exception as e:
            logging.error(f"Error generating SQL query: {e}")
            return "Error generating SQL query."
        
    def pipe(self, user_message: str) -> Union[str, Generator, Iterator]:
        
        try:
            sql_query = self.generate_sql_query(user_message)
        
            return sql_query
        except Exception as e:
            logging.error(f"Error processing request: {e}")
            return f"Error: {e}"



