from uuid import UUID, uuid4
import datetime
from datetime import timezone
from dateutil import parser
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

    def __post_init__(self):
        if isinstance(self.id, str):
            object.__setattr__(self, 'id', UUID(self.id))
        if isinstance(self.created, str):
            object.__setattr__(self, 'created', parser.isoparse(self.created).replace(tzinfo=timezone.utc))
        if isinstance(self.created, str):
            object.__setattr__(self, 'modified', parser.isoparse(self.modified).replace(tzinfo=timezone.utc))


@dataclass
class Genre:
    id: UUID
    name: str
    description: str
    created: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))
    modified: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))

    def __post_init__(self):
        if isinstance(self.id, str):
            object.__setattr__(self, 'id', UUID(self.id))
        if isinstance(self.created, str):
            object.__setattr__(self, 'created', parser.isoparse(self.created).replace(tzinfo=timezone.utc))
        if isinstance(self.created, str):
            object.__setattr__(self, 'modified', parser.isoparse(self.modified).replace(tzinfo=timezone.utc))


@dataclass
class Person:
    id: UUID
    full_name: str
    created: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))
    modified: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))

    def __post_init__(self):
        if isinstance(self.id, str):
            object.__setattr__(self, 'id', UUID(self.id))
        if isinstance(self.created, str):
            object.__setattr__(self, 'created', parser.isoparse(self.created).replace(tzinfo=timezone.utc))
        if isinstance(self.created, str):
            object.__setattr__(self, 'modified', parser.isoparse(self.modified).replace(tzinfo=timezone.utc))


@dataclass
class GenreFilmWork:
    id: UUID
    genre_id: UUID
    film_work_id: UUID
    created: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))

    def __post_init__(self):
        if isinstance(self.id, str):
            object.__setattr__(self, 'id', UUID(self.id))
        if isinstance(self.genre_id, str):
            object.__setattr__(self, 'genre_id', UUID(self.genre_id))
        if isinstance(self.film_work_id, str):
            object.__setattr__(self, 'film_work_id', UUID(self.film_work_id))
        if isinstance(self.created, str):
            object.__setattr__(self, 'created', parser.isoparse(self.created).replace(tzinfo=timezone.utc))


@dataclass
class PersonFilmWork:
    id: UUID
    person_id: UUID
    film_work_id: UUID
    role: str
    created: datetime.datetime = field(default=datetime.datetime.now(datetime.timezone.utc))

    def __post_init__(self):
        if isinstance(self.id, str):
            object.__setattr__(self, 'id', UUID(self.id))
        if isinstance(self.person_id, str):
            object.__setattr__(self, 'person_id', UUID(self.person_id))
        if isinstance(self.film_work_id, str):
            object.__setattr__(self, 'film_work_id', UUID(self.film_work_id))
        if isinstance(self.created, str):
            object.__setattr__(self, 'created', parser.isoparse(self.created).replace(tzinfo=timezone.utc))