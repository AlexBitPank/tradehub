from os import getenv

API_KEY_BIN = getenv("API_KEY_BIN")
SECRET_KEY_BIN = getenv("SECRET_KEY_BIN")
TOKEN = getenv("TOKEN")
CHAT_ID = getenv("CHAT_ID")

DB_HOST = getenv("DB_HOST", "localhost")
DB_USER = getenv("DB_USER", "root")
DB_PASSWORD = getenv("DB_PASSWORD", "")
DB_NAME = getenv("DB_NAME", "database")

REDIS_HOST = getenv("REDIS_HOST")
REDIS_PORT = getenv("REDIS_PORT")
REDIS_PASSWORD = getenv("REDIS_PASSWORD")
