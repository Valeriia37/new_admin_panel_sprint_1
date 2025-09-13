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

    def close_connection(self):
        self.conn.close()

    def get_table_row_count(self, table_name: str) -> int:
        """Получение количества строк в таблице"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self.cursor.execute(query)
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f"Ошибка при получении количества строк в таблице {table_name}: {e}")
            return 0

    def save_data(self, batch: List[FilmWork | Person | Genre],
                  table_name: str, conflict_col: str = 'id'):
        """Сохранение данных в таблицы пачками с обработкой ошибок

        Args:
            batch: Список объектов для сохранения
            table_name: Имя таблицы
            conflict_col: Поля, по которым происходит контроль уникальности
        """

        if not batch:
            logger.info("Нет данных для сохранения")
            return
        
        try:
            column_names = [field.name for field in fields(batch[0])]
            column_names_str = ','.join(column_names)
            col_count = ', '.join(['%s'] * len(column_names))

            self._save_batch(batch, table_name, column_names_str, col_count, conflict_col)

            logger.info(f"Успешно сохранено {len(batch)} записей в таблицу {table_name}")

        except Exception as e:
            logger.error(f"Критическая ошибка при сохранении данных: {e}")
            raise

    def _save_batch(self, batch: List[FilmWork | Person | Genre], table_name: str, 
                    column_names_str: str, col_count: str, conflict_col: str):
        """Сохранение одного пакета данных

        Args:
            batch: Список объектов для сохранения
            table_name: Имя таблицы
            column_names_str: строка со списком наименований колонок
            col_count: строка с %S
            conflict_col: Поля, по которым происходит контроль уникальности
        """
        try:
            bind_values = ','.join(self.cursor.mogrify(f"({col_count})", astuple(item)) for item in batch)

            query = (f"""INSERT INTO {table_name} ({column_names_str}) 
                    VALUES {bind_values} 
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

