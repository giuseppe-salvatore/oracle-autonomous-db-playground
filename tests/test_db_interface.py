from unittest.mock import Mock

import pytest

from lib.database.dbinterface import DBInterface
from lib.database.oracle.dbimpl import OracleDB
from lib.database.sqlite.dbimpl import SQLite3DB


def test_incomplete_implementation_cannot_be_instantiated():
    class IncompleteDatabase(DBInterface):
        def configure(self):
            pass

    with pytest.raises(TypeError, match="abstract"):
        IncompleteDatabase()


@pytest.mark.parametrize("database_class", [OracleDB, SQLite3DB])
def test_database_implements_contract_and_close_is_idempotent(database_class):
    database = database_class.__new__(database_class)
    connection = Mock()
    database.connection = connection
    database.close()
    database.close()
    connection.close.assert_called_once_with()
    assert database.connection is None
