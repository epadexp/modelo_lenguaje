import os
import logging
import ollama
from pydantic import BaseModel
from typing import List, Union, Generator, Iterator

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Valves(BaseModel):
        OPENAI_API_BASE_URL: str = "http://localhost:11434"
        OPENAI_API_KEY: str = ""  # Asegúrate de que esta clave esté configurada correctamente
        OPENAI_API_MODEL: str = "llama3"
        OPENAI_API_TEMPERATURE: float = 0.7
        AGENT_SYSTEM_PROMPT: str = (
            "Eres un generador de consultas SQL para PostgreSQL. "
            "Convierte la siguiente solicitud en una consulta SQL válida y segura. "
            "No agregues explicaciones, solo devuelve la consulta SQL.\n\n"
            "Ejemplo:\n"
            "Entrada: 'Mostrar todos los usuarios activos'\n"
            "Salida: SELECT * FROM users WHERE status = 'active';\n\n"
            "Entrada: '{user_message}'\n"
            "Salida:"
        )

    def __init__(self):
        self.name = "Chat with YouTube"
        self.tools = None
        self.valves = self.Valves(
            OPENAI_API_KEY=os.getenv("OPENAI_API_KEY", "")
        )

    def generate_sql_query(self, user_message: str) -> str:
        prompt = self.valves.AGENT_SYSTEM_PROMPT.format(user_message=user_message)
        
        try:
            response = ollama.chat(  # Uso de Ollama para obtener la respuesta
                model=self.valves.OPENAI_API_MODEL,
                messages=[{"role": "system", "content": prompt}]
            )

            # Imprimir la respuesta completa para depuración
            logging.debug(f"Respuesta completa: {response}")

            # Aquí es donde debes verificar cómo acceder correctamente a la respuesta
            if 'text' in response:
                sql_query = response['text'].strip()
                return sql_query
            else:
                # Si no hay 'text', imprimir toda la respuesta para ver qué campos tiene
                logging.error("La respuesta no contiene el campo 'text'. Respuesta completa:")
                logging.error(response)
                return "Error: La respuesta no contiene 'text'."

        except Exception as e:
            logging.error(f"Error generating SQL query: {e}")
            return f"Error generating SQL query: {e}"

def pipe(self, user_message: str, messages: List[dict], body: dict, model_id: str = None) -> Union[str, Generator, Iterator]:
        
        try:
            sql_query = self.generate_sql_query(user_message)
        
            return sql_query
        except Exception as e:
            logging.error(f"Error processing request: {e}")
            return f"Error: {e}"



