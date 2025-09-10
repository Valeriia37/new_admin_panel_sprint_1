from models import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork
from typing import List
from  dataclasses import fields, astuple
import psycopg

class PostgresSaver:

    def __init__(self, conn):
        self.conn: psycopg.connection = conn
        self.cursor = self.conn.cursor()


    def save_data(self, data_list: List[FilmWork | Person | Genre], table_name: str):
        """Сохранение данных в таблицу фильмов или персон или жанров"""
        column_names = [field.name for field in fields(data_list[0])]
        column_names_str = ','.join(column_names)
        col_count = ', '.join(['%s'] * len(column_names))

        bind_values = ','.join(self.cursor.mogrify(f"({col_count})", astuple(user)) for user in data_list)

        query = (f'INSERT INTO {table_name} ({column_names_str}) VALUES {bind_values} '
                    f'ON CONFLICT (id) DO NOTHING')

        self.cursor.execute(query)

    def save_genre_film_data(self, data_list: List[GenreFilmWork]):
        """Сохранение данных в таблицу жанров фильмов"""
        column_names = [field.name for field in fields(data_list[0])]
        column_names_str = ','.join(column_names)
        col_count = ', '.join(['%s'] * len(column_names))

        bind_values = ','.join(self.cursor.mogrify(f"({col_count})", astuple(user)) for user in data_list)

        query = (f'INSERT INTO content.genre_film_work ({column_names_str}) VALUES {bind_values} '
                    f'ON CONFLICT (genre_id, film_work_id) DO NOTHING')

        self.cursor.execute(query)

    def save_person_film_data(self, data_list: List[PersonFilmWork]):
        """Сохранение данных в таблицу персон фильмов"""
        column_names = [field.name for field in fields(data_list[0])]
        column_names_str = ','.join(column_names)
        col_count = ', '.join(['%s'] * len(column_names))

        bind_values = ','.join(self.cursor.mogrify(f"({col_count})", astuple(user)) for user in data_list)

        query = (f'INSERT INTO content.person_film_work ({column_names_str}) VALUES {bind_values} '
                    f'ON CONFLICT (person_id, film_work_id) DO NOTHING')

        self.cursor.execute(query)