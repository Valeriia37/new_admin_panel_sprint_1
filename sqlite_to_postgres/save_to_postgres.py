import psycopg
import logging
from psycopg import errors as pg_errors
from models import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork
from typing import List
from dataclasses import fields, astuple


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresSaver:

    def __init__(self, conn):
        self.conn: psycopg.connection = conn
        self.cursor = self.conn.cursor()
        self.batch_size = 300

    def set_batch_size(self, size: int):
        """Установка размера пакета"""
        if size > 0:
            self.batch_size = size

    def save_data(self, data_list: List[FilmWork | Person | Genre],
                  table_name: str, conflict_col: str = 'id'):
        """Сохранение данных в таблицы пачками с обработкой ошибок

        Args:
            data_list: Список объектов для сохранения
            table_name: Имя таблицы
        """

        if not data_list:
            logger.info("Нет данных для сохранения")
            return
        
        try:
            column_names = [field.name for field in fields(data_list[0])]
            column_names_str = ','.join(column_names)
            col_count = ', '.join(['%s'] * len(column_names))

            for i in range(0, len(data_list), self.batch_size):
                batch = data_list[i:i + self.batch_size]
                self._save_batch(batch, table_name, column_names_str, col_count, conflict_col)

            logger.info(f"Успешно сохранено {len(data_list)} записей в таблицу {table_name}")

        except Exception as e:
            logger.error(f"Критическая ошибка при сохранении данных: {e}")
            raise

    def _save_batch(self, batch: List[FilmWork | Person | Genre], table_name: str, 
                    column_names_str: str, col_count: str, conflict_col: str):
        """Сохранение одного пакета данных"""
        try:
            bind_values = ','.join(self.cursor.mogrify(f"({col_count})", astuple(item)).decode('utf-8') for item in batch)

            query = (f"""INSERT INTO {table_name} ({column_names_str}) 
                    VALUES {bind_values} '
                    ON CONFLICT ({conflict_col}) DO NOTHING""")

            self.cursor.execute(query)
            logger.debug(f"Сохранен пакет из {len(batch)} записей в {table_name}")

        except pg_errors.DeadlockDetected:
            logger.warning(f"Обнаружена взаимная блокировка")
            self.conn.rollback()

        except pg_errors.UniqueViolation as e:
            logger.warning(f"Нарушение уникальности: {e}")
            self.conn.rollback()

        except pg_errors.ForeignKeyViolation as e:
            logger.error(f"Нарушение внешнего ключа: {e}")
            self.conn.rollback()
        
        except (pg_errors.OperationalError, pg_errors.InterfaceError) as e:
            logger.error(f"Ошибка соединения")
            
        except Exception as e:
            logger.error(f"Неожиданная ошибка при сохранении пакета: {e}")
            self.conn.rollback()

