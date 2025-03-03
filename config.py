from os import getenv


api_key_bin = getenv("API_KEY_BIN")
secret_key_bin = getenv("SECRET_KEY_BIN")
token = getenv("TOKEN")
chat_id = getenv("CHAT_ID")

db_host = getenv("DB_HOST", "localhost")
db_user = getenv("DB_USER", "root")
db_password = getenv("DB_PASSWORD", "")
db_name = getenv("DB_NAME", "database")

redis_host = getenv("REDIS_HOST")
redis_port = getenv("REDIS_PORT")
redis_password = getenv("REDIS_PASSWORD")
