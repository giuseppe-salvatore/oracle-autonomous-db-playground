import sqlite3

from sqlite3 import Error
from lib.utils.logger import log
from sqlite3.dbapi2 import Cursor
from lib.database.dbinterface import DBInterface
from lib.utils.env import read_environment_variable


class SQLite3DB(DBInterface):

    def __init__(self):
        self.configure()

    def configure(self):
        log.debug("Configuring SQLite3 Database")
        self.db_file = read_environment_variable("SQLITE_DB_FILE")
        log.debug("Successfully set SQLite3 Database configuration")

    def connect(self):
        """ create a database connection to a SQLite database """
        self.connection = None
        try:
            self.connection = sqlite3.connect(self.db_file)
        except Error as e:
            log.error(e)

    def execute_sql(self, query: str) -> list:
        try:
            cursor = self.connection.cursor()
            ret: Cursor = cursor.execute(query)
            return ret.fetchall()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()

    def execute_ddl(self, query: str):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)

            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e

    def execute_many_queries(self, query: str, rows):
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, rows)
            log.debug(cursor.rowcount, "Rows Inserted")

            self.connection.commit()

        except Exception as e:
            self.connection.rollback()
            raise e

    def get_connection(self):
        return self.connection
