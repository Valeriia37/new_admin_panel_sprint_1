import sqlite3
import logging
from uuid import UUID
from models import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLiteLoader:

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def _execute_query(self, query: str, params: tuple = None) -> List[sqlite3.Row]:
        """Выполнение SQL запроса с обработкой ошибок"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            logger.error(f"Запрос: {query}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при выполнении запроса: {e}")
            raise

    def load_film_work(self) -> List[FilmWork]:
        """Загрузка данных из таблицы с фильмами"""
        self._execute_query('SELECT id, ' \
                            'title, description, ' \
                            'creation_date, rating,' \
                            'type ' \
                            'FROM film_work')
        data: list[FilmWork] = []
        results = self.cursor.fetchall()
        for result in results:
            try:
                result = dict(result)
                film = FilmWork(id=UUID(result['id']),
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
        return data
    
    def load_person(self) -> List[Person]:
        """Загрузка данных из таблицы с персонами"""
        self.cursor._execute_query('SELECT * FROM person')
        data: list[Person] = []
        results = self.cursor.fetchall()
        for result in results:
            try:
                result = dict(result)
                person = Person(id=UUID(result['id']),
                            full_name=result['full_name'])
                data.append(person)
            except Exception as e:
                logger.error(f"Ошибка обработки строки персоны: {e}")
                logger.error(f"Данные строки: {dict(result)}")
                continue
        logger.info(f"Успешно загружено {len(data)} записей из person")
        return data
    
    def load_genre(self) -> List[Genre]:
        """Загрузка данных из таблицы с жанрами"""
        self.cursor._execute_query('SELECT * FROM genre')
        data: list[Genre] = []
        results = self.cursor.fetchall()
        for result in results:
            try:
                result = dict(result)
                genre = Genre(id=UUID(result['id']),
                            name=result['name'],
                            description=result['description'])
                data.append(genre)
            except Exception as e:
                logger.error(f"Ошибка обработки строки жанра: {e}")
                logger.error(f"Данные строки: {dict(result)}")
                continue
        logger.info(f"Успешно загружено {len(data)} записей из genre")
        return data
    
    def load_genre_fim_work(self) -> List[GenreFilmWork]:
        """Загрузка данных из таблицы с жанрами фильмов"""
        self.cursor._execute_query('SELECT * FROM genre_film_work')
        data: list[GenreFilmWork] = []
        results = self.cursor.fetchall()
        for result in results:
            try:
                result = dict(result)
                genre_film = GenreFilmWork(id=UUID(result['id']),
                                        genre_id=UUID(result['genre_id']),
                                        film_work_id=UUID(result['film_work_id']))
                data.append(genre_film)
            except Exception as e:
                logger.error(f"Ошибка обработки строки genre_film_work: {e}")
                logger.error(f"Данные строки: {dict(result)}")
                continue
        logger.info(f"Успешно загружено {len(data)} записей из genre_film_work")
        return data
    
    def load_person_fim_work(self) -> List[PersonFilmWork]:
        """Загрузка данных из таблицы с персонами фильмов"""
        self.cursor._execute_query('SELECT * FROM person_film_work')
        data: list[PersonFilmWork] = []
        results = self.cursor.fetchall()
        for result in results:
            try:
                result = dict(result)
                person_film = PersonFilmWork(id=UUID(result['id']),
                                            person_id=UUID(result['person_id']),
                                            film_work_id=UUID(result['film_work_id']),
                                            role=result['role'])
                data.append(person_film)
            except Exception as e:
                logger.error(f"Ошибка обработки строки person_film_work: {e}")
                logger.error(f"Данные строки: {dict(result)}")
                continue
        
        logger.info(f"Успешно загружено {len(data)} записей из person_film_work")
        return data
