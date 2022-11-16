import sqlite3
from sqlite3 import Error


def create_db_connection(file):

    # create connection to the sqlite database
    try:
        connection = sqlite3.connect(file)
        # print(sqlite3.version)
        return connection
    except Error as e:
        return "Could not connect!"

