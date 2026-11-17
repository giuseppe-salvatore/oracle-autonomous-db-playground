import oracledb
from oracledb.cursor import Cursor
from lib.utils.logger import log
from lib.database.dbinterface import DBInterface
from lib.utils.env import read_environment_variable


class OracleDB(DBInterface):
    def __init__(self):
        self.configure()

    def configure(self):
        log.debug("Configuring Oracle Autonomous Database")

        self.user = read_environment_variable("ORACLE_ADB_USER")
        self.password = read_environment_variable("ORACLE_ADB_PASSWORD")
        self.dsn_tls = read_environment_variable("ORACLE_ADB_TLS_CONNECTION_STRING")
        self.config_dir = read_environment_variable("ORACLE_ADB_WALLET_DIR")
        self.wallet_location = read_environment_variable("ORACLE_ADB_WALLET_DIR")
        self.wallet_password = read_environment_variable("ORACLE_ADB_WALLET_PASSWORD")

        log.debug("Successfully set Oracle Autonomous Database configuration")

    def connect(self):
        self.connection = oracledb.connect(
            user=self.user,
            password=self.password,
            dsn=self.dsn_tls,
            config_dir=self.config_dir,
            wallet_location=self.wallet_location,
            wallet_password=self.wallet_password,
        )

    def execute_sql(self, query: str, parameters=None) -> Cursor:
        try:
            with self.connection.cursor() as cursor:
                ret: Cursor = cursor.execute(query, parameters or {})
                return ret.fetchall()
        except Exception as e:
            self.connection.rollback()
            raise e

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
            with self.connection.cursor() as cursor:
                cursor.executemany(query, rows)
                log.debug(cursor.rowcount, "Rows Inserted")

            self.connection.commit()

        except Exception as e:
            self.connection.rollback()
            raise e

    def get_connection(self):
        return self.connection
