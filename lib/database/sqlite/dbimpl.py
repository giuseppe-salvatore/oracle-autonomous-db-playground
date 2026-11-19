import sqlite3
import re

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
        """create a database connection to a SQLite database"""
        self.connection = None
        try:
            self.connection = sqlite3.connect(self.db_file)
        except Error as e:
            log.error(e)

    def execute_sql(self, query: str, parameters=None) -> list:
        try:
            cursor = self.connection.cursor()
            ret: Cursor = cursor.execute(query, parameters or {})
            return ret.fetchall()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()

    def execute_ddl(self, query: str):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()

    def execute_many_queries(self, query: str, rows):
        cursor = self.connection.cursor()
        try:
            sqlite_query = re.sub(r":\d+", "?", query)
            cursor.executemany(sqlite_query, rows)
            log.debug(cursor.rowcount, "Rows Inserted")

            self.connection.commit()

        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()

    def get_connection(self):
        return self.connection

    def close(self):
        if getattr(self, "connection", None) is not None:
            self.connection.close()
            self.connection = None
