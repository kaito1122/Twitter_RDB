"""
filename: dbutils.py
Requires the driver:  conda install mysql-connector-python

description: A collection of database utilities to make it easier
to implement a database application
"""

import sqlite3
import pandas as pd

class DBUtils:

    """
    def __init__(self, user, password, database, host="localhost"):
        # Future work: Implement connection pooling
        self.con = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
    """

    def __init__(self, path):
        self.con = sqlite3.connect(path)

    def close(self):
        """ Close or release a connection back to the connection pool """
        self.con.close()
        self.con = None

    def execute(self, query):
        """ Execute a select query and returns the result as a dataframe """

        # Step 1: Create cursor
        rs = self.con.cursor()

        # Step 2: Execute the query
        rs.execute(query)

        # Step 3: Get the resulting rows and column names
        rows = rs.fetchall()
        cols = [desc[0] for desc in rs.description]

        # Step 4: Close the cursor
        rs.close()

        # Step 5: Return result
        return pd.DataFrame(rows, columns=cols)

    def insert_one(self, sql, val):
        """ Insert a single row """
        cursor = self.con.cursor()
        cursor.execute(sql, val)
        self.con.commit()


    def insert_many(self, sql, vals):
        """ Insert multiple rows """
        cursor = self.con.cursor()
        cursor.executemany(sql, vals)
        self.con.commit()

    def create_table(self, sql):
        cursor = self.con.cursor()
        cursor.execute(sql)
        self.con.commit()

        cursor.close()
