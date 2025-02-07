import logging

import sqlite3
from sqlite3 import Error

from contextlib import closing

logger = logging.getLogger(__name__)

class DbRepository:
    """Creates a connection to a database. Will create the database if it does not exist."""
    
    def __init__(self, sql_db_path):
        self._sql_db_path = sql_db_path
        self._connection_status = self.create_connection()

    def create_connection(self):
        """Creates a connection to the database. This creates a connection to the database if it exists, if it does not exist it will create it."""

        connection = None
        success = None

        try:
            connection = sqlite3.connect(self._sql_db_path)
            logging.info("Connection to SQLite DB successful")
            success = True
        except Error as e:
            logging.info(f"The error '{e}' occurred")
            success = False

        connection.close()

        return success
    
    def execute_query(self, query, data=[], headers=False):
        """Executes a query against the database. Returns a boolean representing the success/failure of the queries execution."""
        with closing(sqlite3.connect(self._sql_db_path)) as connection:
            with closing(connection.cursor()) as cursor:
                try:
                    if len(data) == 1:
                        cursor.execute(query, data[0])
                    elif len(data) > 1:
                        cursor.executemany(query, data)
                    else:
                        cursor.execute(query)
                    
                    connection.commit()

                    print("Query executed successfully")

                    if headers:
                        headers = list(map(lambda attr : attr[0], cursor.description))
                        results = [{header:row[i] for i, header in enumerate(headers)} for row in cursor]
                        return True, results
                    else:
                        return True, cursor.fetchall()

                except Error as e:
                    print(f"The error '{e}' occurred")

                    return False, None

    def execute_query_from_file(self, file_path, data=[], headers=False):
        """Executes a query against the database using the provided file as the query. Returns a boolean representing the success/failure of the queries execution."""
        query_string = ""
        with open(file_path) as fo:
            query_string = fo.read()

        return self.execute_query(query_string, data, headers)