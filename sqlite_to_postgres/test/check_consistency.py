
import pytest
import psycopg
import os
import sqlite3
from dotenv import load_dotenv
load_dotenv() 

from load_from_sqlite import SQLiteLoader
from save_to_postgres import PostgresSaver
from models import FilmWork, Person, Genre, PersonFilmWork, GenreFilmWork
from psycopg import ClientCursor, connection as _connection
from psycopg.rows import dict_row




@pytest.fixture
def sqlite():
    with sqlite3.connect(fr"{os.environ.get('FILE_PATH')}") as conn:
        sqlite_db = SQLiteLoader(conn)
        yield sqlite_db


@pytest.fixture
def postgres():
    dsl = {'dbname': os.environ.get('DB_NAME'),
           'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'),
           'host': os.environ.get('DB_HOST', '127.0.0.1'),
           'port': os.environ.get('DB_PORT', 5432),
           'options':'-c client_encoding=UTF8'}
    with psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=ClientCursor
    ) as conn:
        postgres_db = PostgresSaver(conn)
        yield postgres_db


@pytest.mark.parametrize('table_name', ['film_work', 'genre', 'person', 'genre_film_work', 'person_film_work'])
def test_rows_in_tables_in_two_db_is_same(sqlite: SQLiteLoader, postgres: PostgresSaver, table_name):
    sqlite_count = sqlite.get_table_row_count(table_name)
    postgres_count = postgres.get_table_row_count(f"content.{table_name}")
    assert sqlite_count == postgres_count


def test_film_work_data_in_two_db_is_same(sqlite: SQLiteLoader, postgres: PostgresSaver):
    for original_batch in sqlite.load_film_work():
        ids = [item.id for item in original_batch]
        postgres.cursor.execute('SELECT * FROM content.film_work WHERE id = ANY(%s)', [ids])
        transferred_batch = [FilmWork(**item) for item in postgres.cursor.fetchall()]
        assert original_batch == transferred_batch 


def test_person_data_in_two_db_is_same(sqlite: SQLiteLoader, postgres: PostgresSaver):
    for original_batch in sqlite.load_person():
        ids = [item.id for item in original_batch]
        postgres.cursor.execute('SELECT * FROM content.person WHERE id = ANY(%s)', [ids])
        transferred_batch = [Person(**item) for item in postgres.cursor.fetchall()]
        assert original_batch == transferred_batch 


def test_genre_data_in_two_db_is_same(sqlite: SQLiteLoader, postgres: PostgresSaver):
    for original_batch in sqlite.load_genre():
        ids = [item.id for item in original_batch]
        postgres.cursor.execute('SELECT * FROM content.genre WHERE id = ANY(%s)', [ids])
        transferred_batch = [Person(**item) for item in postgres.cursor.fetchall()]
        assert original_batch == transferred_batch 


def test_genre_film_work_data_in_two_db_is_same(sqlite: SQLiteLoader, postgres: PostgresSaver):
    for original_batch in sqlite.load_genre_fim_work():
        ids = [item.id for item in original_batch]
        postgres.cursor.execute('SELECT * FROM content.genre_film_work WHERE id = ANY(%s)', [ids])
        transferred_batch = [Person(**item) for item in postgres.cursor.fetchall()]
        assert original_batch == transferred_batch 


def test_person_film_work_data_in_two_db_is_same(sqlite: SQLiteLoader, postgres: PostgresSaver):
    for original_batch in sqlite.load_person_fim_work():
        ids = [item.id for item in original_batch]
        postgres.cursor.execute('SELECT * FROM content.person_film_work WHERE id = ANY(%s)', [ids])
        transferred_batch = [Person(**item) for item in postgres.cursor.fetchall()]
        assert original_batch == transferred_batch 