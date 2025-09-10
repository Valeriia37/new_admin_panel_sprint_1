from uuid import UUID, uuid4
import datetime
# from typing import Optional
from dataclasses import dataclass, field


@dataclass
class FilmWork:
    id: UUID
    title: str
    description: str
    creation_date: datetime.date | None
    rating: float = field(default=0.0)
    type: str = field(default='movie')
    created: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))
    modified: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))


@dataclass
class Genre:
    id: UUID
    name: str
    description: str
    created: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))
    modified: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))


@dataclass
class Person:
    id: UUID
    full_name: str
    created: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))


@dataclass(frozen=True)
class GenreFilmWork:
    id: UUID
    genre_id: UUID
    film_work_id: UUID
    created: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))


@dataclass(frozen=True)
class PersonFilmWork:
    id: UUID
    person_id: UUID
    film_work_id: UUID
    role: str
    created: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))