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