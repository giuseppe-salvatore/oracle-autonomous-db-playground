from abc import ABC, abstractmethod


class DBInterface(ABC):
    """Operations required by database consumers."""

    @abstractmethod
    def configure(self):
        """Load database configuration."""

    @abstractmethod
    def connect(self):
        """Open a connection."""

    @abstractmethod
    def execute_sql(self, query, parameters=None):
        """Run a query and return its rows."""

    @abstractmethod
    def execute_ddl(self, query):
        """Execute a schema statement."""

    @abstractmethod
    def execute_many_queries(self, query, rows):
        """Execute a statement for multiple rows."""

    @abstractmethod
    def get_connection(self):
        """Return the underlying connection."""

    @abstractmethod
    def close(self):
        """Close the connection if one is open."""
