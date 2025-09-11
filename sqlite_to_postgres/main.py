import sqlite3
import os
import psycopg
import logging

from dotenv import load_dotenv
from psycopg import ClientCursor, connection as _connection
from psycopg.rows import dict_row

load_dotenv() 

from load_from_sqlite import SQLiteLoader
from save_to_postgres import PostgresSaver

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_from_sqlite_to_postgres(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    data: list = sqlite_loader.load_film_work()
    postgres_saver.save_data(data, 'content.film_work')

    data: list = sqlite_loader.load_genre()
    postgres_saver.save_data(data, 'content.genre')

    data: list = sqlite_loader.load_person()
    postgres_saver.save_data(data, 'content.person')

    data: list = sqlite_loader.load_genre_fim_work()
    postgres_saver.save_data(data, 'content.genre_film_work', 'genre_id, film_work_id')

    data: list = sqlite_loader.load_person_fim_work()
    postgres_saver.save_data(data,'content.person_film_work', 'person_id, film_work_id')

    logging.info('Данные из sqlite загружены в postgres')


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'),
           'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'),
           'host': os.environ.get('DB_HOST', '127.0.0.1'),
           'port': os.environ.get('DB_PORT', 5432),
           'options':'-c client_encoding=UTF8'}
    try:
        with sqlite3.connect(fr"{os.environ.get('FILE_PATH')}") as sqlite_conn, psycopg.connect(
            **dsl, row_factory=dict_row, cursor_factory=ClientCursor
        ) as pg_conn:
            load_from_sqlite_to_postgres(sqlite_conn, pg_conn)
    except sqlite3.Error as e:
        logging.error(f"Ошибка подключения к SQLite: {e}")
    except FileNotFoundError:
        logging.error("Файл SQLite не найден")
    except PermissionError:
        logging.error("Нет прав доступа к файлу SQLite")
    except psycopg.OperationalError as e:
        logging.error(f"Ошибка подключения к PostgreSQL: {e}")
    except psycopg.InterfaceError as e:
        logging.error(f"Ошибка интерфейса PostgreSQL: {e}")
    except Exception as e:
        logging.error(f'Непредвиденная ошибка при подключении к базам данных: е={e}.')