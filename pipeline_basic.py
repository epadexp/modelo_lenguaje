import os
import logging


from pydantic import BaseModel
from typing import List


logging.basicConfig(level=logging.DEBUG)

class Pipeline:

    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str
        DB_DATABASE: str
        DB_TABLES: List[str]
        VLLM_HOST: str
        OPENAI_API_KEY: str
        TEXT_TO_SQL_MODEL: str

    def __init__(self):
        self.name = "01 Database RAG Pipeline vLLM llama"
        self.conn = None
        self.nlsql_response = ""

        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "DB_HOST": os.getenv("PG_HOST", "http://10.30.164.243"),
                "DB_PORT": os.getenv("PG_PORT", '5432'),
                "DB_USER": os.getenv("PG_USER", "admin"),
                "DB_PASSWORD": os.getenv("PG_PASSWORD", "tester123"),
                "DB_DATABASE": os.getenv("PG_DB", "postgres"),
                "DB_TABLES": ["movies"],
                "VLLM_HOST": os.getenv("VLLM_HOST", "http://10.1.152.55:8012/v1"),
                "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", 'abc-123'),
                "TEXT_TO_SQL_MODEL": "NousResearch/Meta-Llama-3-8B-Instruct"
            }
        )