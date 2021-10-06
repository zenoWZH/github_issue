from config import database
from config.database import HOST, PORT, USER, PASSWORD, DATABASE
import psycopg2

db = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE
        #charset='utf8'
)