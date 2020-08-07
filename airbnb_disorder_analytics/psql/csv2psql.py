from airbnb_disorder_analytics.config.db_config import DBInfo
import psycopg2

connection = psycopg2.connect(DBInfo.psycopg2_config)
cursor = connection.cursor()