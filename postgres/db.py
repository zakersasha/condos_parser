import psycopg2

from config import Config

connection = psycopg2.connect(
    host=Config.DB_HOST,
    database=Config.DB_NAME,
    user=Config.DB_USERNAME,
    password=Config.DB_PASSWORD,

)
