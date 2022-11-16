import sqlite3
from sqlite3 import Error
from databaseconnection import create_db_connection


def create_sql_statement(table_name, schema_json):
    schema = ", ".join([f"{i} {j}" for i, j in schema_json.items()])
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
    return sql


class Database:
    """ create a database connection to sqlite and
        run create and insert queries

    :param filename: database file name
    """

    def __init__(self, filename):
        self.filename = filename
        self.db_connection = create_db_connection(self.filename)

    def create_table(self, sql_create_table):
        """create sql table in the database
        :param sql_create_table: sql create table statement
        :return Successful or not
        """

        try:
            c = self.db_connection.cursor()
            c.execute(sql_create_table)
            c.close()
            return "Successful!"

        except Error as e:
            print(e)

    def close_connection(self):
        self.db_connection.close()
