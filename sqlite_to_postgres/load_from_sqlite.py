import sqlite3
import logging
from uuid import UUID
from models import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork
from typing import List, Generator
from dataclasses import fields

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLiteLoader:

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.batch_size = 100
        self.transform_col_name = {'modified':'updated_at',
                                   'created':'created_at'}
        self.table_class_map = {
            'person': Person,
            'film_work': FilmWork,
            'genre': Genre,
            'genre_film_work': GenreFilmWork,
            'person_film_work': PersonFilmWork
        }

    def close_connection(self):
        self.conn.close()

    def set_batch_size(self, size: int):
        """Установка размера пакета"""
        if size > 0:
            self.batch_size = size

    def get_table_row_count(self, table_name: str) -> int:
        """Получение количества строк в таблице"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self.cursor.execute(query)
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f"Ошибка при получении количества строк в таблице {table_name}: {e}")
            return 0

    def _execute_query(self, query: str, params: tuple = None) -> Generator[List[sqlite3.Row], None, None]:
        """Выполнение SQL запроса с обработкой ошибок"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            while results := self.cursor.fetchmany(self.batch_size):
                yield results 
        except sqlite3.Error as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            logger.error(f"Запрос: {query}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при выполнении запроса: {e}")
            raise

    def load_data(self, table_name) -> Generator[List[FilmWork | Person | Genre | PersonFilmWork | GenreFilmWork], None, None]:
        """Загрузка данных из таблицы table_name"""
        if table_name not in self.table_class_map.keys():
            logger.error(f"Попытка загрузки данных с незарегистрированной таблицы - {table_name}")
            raise ValueError(f"Попытка загрузки данных с незарегистрированной таблицы - {table_name}")
        
        object_class = self.table_class_map[table_name]
        need_columns = [f.name for f in fields(object_class)]
        fields_str = ''
        for col in need_columns:
            if col in self.transform_col_name.keys():
                fields_str += f"{self.transform_col_name[col]} as {col}, "
            else:
                fields_str += f"{col}, "
        fields_str = fields_str[:-2]
        
        for batch in self._execute_query(f'SELECT {fields_str} FROM {table_name}'):
            data: List[FilmWork | Person | Genre | PersonFilmWork | GenreFilmWork] = []
            for result in batch:
                try:
                    result = dict(result)
                    film = object_class(**result)
                    data.append(film)
                except Exception as e:
                    logger.error(f"Ошибка обработки строки: {e}")
                    logger.error(f"Данные строки: {dict(result)}")
                    continue
            logger.info(f"Успешно загружено {len(data)} записей из {table_name}")
            yield data
