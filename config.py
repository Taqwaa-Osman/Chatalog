# config.py
import os

NEO4J_URI = os.getenv("NEO4J_URI", "neo4j+s://2ec65fcd.databases.neo4j.io")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "aFhEUnY8Wp05kqpx3-NLq9UHnPJO5mFY04pPNy1-1ag")
DATABASE = os.getenv("DATABASE", "neo4j")

COHERE_API_KEY = os.getenv("COHERE_API_KEY", "6aYMtUTnbSWYXtkCo6NvIOCt0p587KCohmfUsiHS")
COHERE_MODEL = os.getenv("COHERE_MODEL", "command-r-plus-08-2024")
