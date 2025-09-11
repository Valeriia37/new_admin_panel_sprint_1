import sqlite3
import logging
from uuid import UUID
from models import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork
from typing import List, Generator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLiteLoader:

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.batch_size = 100

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

    def _execute_query(self, query: str, params: tuple = None) -> Generator[List[sqlite3.Row]]:
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

    def load_film_work(self) -> Generator[List[FilmWork]]:
        """Загрузка данных из таблицы с фильмами"""
        for batch in self._execute_query('SELECT id, ' \
                                        'title, description, ' \
                                        'creation_date, rating,' \
                                        'type ' \
                                        'FROM film_work'):
            data: list[FilmWork] = []
            for result in batch:
                try:
                    result = dict(result)
                    film = FilmWork(id=result['id'],
                                    title=result['title'],
                                    description=result['description'],
                                    creation_date=result['creation_date'],
                                    rating=result['rating'],
                                    type=result['type'])
                    data.append(film)
                except Exception as e:
                    logger.error(f"Ошибка обработки строки фильма: {e}")
                    logger.error(f"Данные строки: {dict(result)}")
                    continue
            logger.info(f"Успешно загружено {len(data)} записей из film_work")
            yield data
    
    def load_person(self) -> Generator[List[Person]]:
        """Загрузка данных из таблицы с персонами"""
        for batch in self._execute_query('SELECT * FROM person'):
            data: list[Person] = []
            for result in batch:
                try:
                    result = dict(result)
                    person = Person(id=result['id'],
                                full_name=result['full_name'])
                    data.append(person)
                except Exception as e:
                    logger.error(f"Ошибка обработки строки персоны: {e}")
                    logger.error(f"Данные строки: {dict(result)}")
                    continue
            logger.info(f"Успешно загружено {len(data)} записей из person")
            yield data
    
    def load_genre(self) -> Generator[List[Genre]]:
        """Загрузка данных из таблицы с жанрами"""
        for batch in self._execute_query('SELECT * FROM genre'):
            data: list[Genre] = []
            for result in batch:
                try:
                    result = dict(result)
                    genre = Genre(id=result['id'],
                                name=result['name'],
                                description=result['description'])
                    data.append(genre)
                except Exception as e:
                    logger.error(f"Ошибка обработки строки жанра: {e}")
                    logger.error(f"Данные строки: {dict(result)}")
                    continue
            logger.info(f"Успешно загружено {len(data)} записей из genre")
            yield data
    
    def load_genre_fim_work(self) -> Generator[List[GenreFilmWork]]:
        """Загрузка данных из таблицы с жанрами фильмов"""
        for batch in self._execute_query('SELECT * FROM genre_film_work'):
            data: list[GenreFilmWork] = []
            for result in batch:
                try:
                    result = dict(result)
                    genre_film = GenreFilmWork(id=result['id'],
                                            genre_id=result['genre_id'],
                                            film_work_id=result['film_work_id'])
                    data.append(genre_film)
                except Exception as e:
                    logger.error(f"Ошибка обработки строки genre_film_work: {e}")
                    logger.error(f"Данные строки: {dict(result)}")
                    continue
            logger.info(f"Успешно загружено {len(data)} записей из genre_film_work")
            yield data
    
    def load_person_fim_work(self) -> Generator[List[PersonFilmWork]]:
        """Загрузка данных из таблицы с персонами фильмов"""
        for batch in self._execute_query('SELECT * FROM person_film_work'):
            data: list[PersonFilmWork] = []
            for result in batch:
                try:
                    result = dict(result)
                    person_film = PersonFilmWork(id=result['id'],
                                                person_id=result['person_id'],
                                                film_work_id=result['film_work_id'],
                                                role=result['role'])
                    data.append(person_film)
                except Exception as e:
                    logger.error(f"Ошибка обработки строки person_film_work: {e}")
                    logger.error(f"Данные строки: {dict(result)}")
                    continue
        
            logger.info(f"Успешно загружено {len(data)} записей из person_film_work")
            yield data
