import requests
import logging
from typing import List, Union, Generator, Iterator


class Pipeline:
    def __init__(self):
        self.url = "http://127.0.0.1:11434/v1/chat/completions"
        self.headers = {
            "Content-Type": "application/json"
        }
        self.model = "llama3"  # Modelo que estás usando, cambia según corresponda

    def generate_sql_query(self, user_message: str) -> str:
        prompt = (
            "Eres un generador de consultas SQL para PostgreSQL. "
            "Convierte la siguiente solicitud en una consulta SQL válida y segura. "
            "No agregues explicaciones, solo devuelve la consulta SQL.\n\n"
            f"Entrada: '{user_message}'\nSalida:"
        )
        
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
        

    def pipe(self, user_message: str, messages: List[dict], body: dict, model_id: str = None) -> Union[str, Generator, Iterator]:
        
        try:
            sql_query = self.generate_sql_query(user_message)
        
            return sql_query
        except Exception as e:
            logging.error(f"Error processing request: {e}")
            return f"Error: {e}"

# Prueba con un mensaje de usuario
# pipeline = Pipeline()
# user_message = "Mostrar todos los registros de la tabla embeddings"
# sql_query = pipeline.generate_sql_query(user_message)
# print("Consulta SQL generada:", sql_query)





