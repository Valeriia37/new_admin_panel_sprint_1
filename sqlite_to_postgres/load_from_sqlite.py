import sqlite3
from uuid import UUID
from models import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork
from typing import List

class SQLiteLoader:

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def load_film_work(self) -> List[FilmWork]:
        """Загрузка данных из таблицы с фильмами"""
        self.cursor.execute('SELECT id, ' \
                            'title, description, ' \
                            'creation_date, rating,' \
                            'type ' \
                            'FROM film_work')
        data: list[FilmWork] = []
        results = self.cursor.fetchall()
        for result in results:
            result = dict(result)
            film = FilmWork(id=UUID(result['id']),
                            title=result['title'],
                            description=result['description'],
                            creation_date=result['creation_date'],
                            rating=result['rating'],
                            type=result['type'])
            data.append(film)
        return data
    
    def load_person(self) -> List[Person]:
        """Загрузка данных из таблицы с персонами"""
        self.cursor.execute('SELECT * FROM person')
        data: list[Person] = []
        results = self.cursor.fetchall()
        for result in results:
            result = dict(result)
            person = Person(id=UUID(result['id']),
                          full_name=result['full_name'])
            data.append(person)
        return data
    
    def load_genre(self) -> List[Genre]:
        """Загрузка данных из таблицы с жанрами"""
        self.cursor.execute('SELECT * FROM genre')
        data: list[Genre] = []
        results = self.cursor.fetchall()
        for result in results:
            result = dict(result)
            genre = Genre(id=UUID(result['id']),
                          name=result['name'],
                          description=result['description'])
            data.append(genre)
        return data
    
    def load_genre_fim_work(self) -> List[GenreFilmWork]:
        """Загрузка данных из таблицы с жанрами фильмов"""
        self.cursor.execute('SELECT * FROM genre_film_work')
        data: list[GenreFilmWork] = []
        results = self.cursor.fetchall()
        for result in results:
            result = dict(result)
            genre_film = GenreFilmWork(id=UUID(result['id']),
                                       genre_id=UUID(result['genre_id']),
                                       film_work_id=UUID(result['film_work_id']))
            data.append(genre_film)
        return data
    
    def load_person_fim_work(self) -> List[PersonFilmWork]:
        """Загрузка данных из таблицы с персонами фильмов"""
        self.cursor.execute('SELECT * FROM person_film_work')
        data: list[PersonFilmWork] = []
        results = self.cursor.fetchall()
        for result in results:
            result = dict(result)
            person_film = PersonFilmWork(id=UUID(result['id']),
                                         person_id=UUID(result['person_id']),
                                         film_work_id=UUID(result['film_work_id']),
                                         role=result['role'])
            data.append(person_film)
        return data
