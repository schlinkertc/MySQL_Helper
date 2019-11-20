import mysql.connector as mysql
from mysql.connector import errorcode
from mysql.connector import Error
import pandas as pd
import numpy as np

class Connection:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password

        self.cnx = mysql.connect(
        host = self.host,
        user = self.user,
        passwd = self.password)

        self.cursor = self.cnx.cursor()

    def add_DB(self,DB_name):
        try:
            query = """CREATE DATABASE IF NOT EXISTS {}""".format(DB_name)
            print(query)
        except mysql.Error as err:
            print(err.msg)

        self.cursor.execute(query)

    def drop_DB(self,DB_name):
        query = """DROP DATABASE {}""".format(DB_name)
        print(query)

        self.cursor.execute(query)

    def query(self,query):
        #self.connect(self)
        self.cursor.execute(query)

        return self.cursor.fetchall()

class DataBase(Connection):
    def __init__(self,Connection, DB_name):
        self.host = Connection.host
        self.user = Connection.user
        self.password = Connection.password
        self.DB_name = DB_name

        self.cnx = mysql.connect(
        host = self.host,
        user = self.user,
        passwd = self.password,
        database = self.DB_name)

        self.cursor = self.cnx.cursor()

    def connect(self):
        self.cnx = mysql.connect(
        host = self.host,
        user = self.user,
        passwd = self.password,
        database = self.DB_name)

        self.cursor = self.cnx.cursor()

    def query(self,query):
        if self.cnx.is_connected()==False:
            self.connect()
        self.cursor.execute(query)

        return self.cursor.fetchall()

        self.cursor.close()
        self.cnx.close()

    def query_to_df(self,query):
        if self.cnx.is_connected()==False:
            self.connect()

        self.cursor.execute(query)

        df = pd.DataFrame(self.cursor.fetchall())
        df.columns = [x[0] for x in self.cursor.description]
        return df

        self.cursor.close()
        self.cnx.close()

    def tables(self):
        if self.cnx.is_connected()==False:
            self.connect()

        tables = DataBase.query(self,"""show tables""")
        return [item for sublist in tables for item in sublist]

    def create_table(self, query):
        if self.cnx.is_connected()==False:
            self.connect()

        try:
            print("Creating a new table")
            self.cursor.execute(query)
        except mysql.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

        self.cursor.close()
        self.cnx.close()

    def table_fromDf(self,df,table_name,*primary_key):
        if self.cnx.is_connected()==False:
            self.connect()

        pandas_sql_dtypes = ({np.dtype('int64'):'INT (64)',
                          np.dtype('O'):'VARCHAR (250)',
                          np.dtype('<M8[ns]'):'DATETIME (6)'})
        a = list(df.columns)
        b = [df[x].dtype for x in a]
        c = [pandas_sql_dtypes.get(x,'NULL') for x in b]
        e = list(zip(a,c))
        f = "".join([x[0]+' '+x[1]+', ' for x in e])
        if primary_key:
            g = ("CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}));".format(
            table_name,f[0:-2],primary_key[0]))
            print(g)
        else:
            g = ("CREATE TABLE IF NOT EXISTS {} ({});".format(table_name,f[0:-2]))
            print(g)
        self.create_table(g)

    def insert_fromDf(self,df,table_name):
        if self.cnx.is_connected()==False:
            self.connect()

        statement = ("""INSERT IGNORE INTO {}({}) VALUES({})""".format(
        table_name,", ".join(list(df.columns)),
        ('%s, '*len(df.columns))[:-2]))

        data = [tuple(x) for x in df.values]

        self.cursor.executemany(statement, data)
        self.cnx.commit()

    def insert_fromDf_iteration(self,df,table_name):
        if self.cnx.is_connected()==False:
            self.connect()

        statement = ("""INSERT IGNORE INTO {}({}) VALUES({})""".format(
        table_name,", ".join(list(df.columns)),
        ('%s, '*len(df.columns))[:-2]))

        data = [tuple(x) for x in df.values]

        for item in data:
            try:
                self.connect()
                self.cursor.execute(statement,item)
                self.cnx.commit()
                print("{} succesfully inserted".format(item[:5]))
            except mysql.Error as err:
                print("Failed to insert {} into Laptop table {}".format(item[:5],err.msg))

        if (self.cnx.is_connected()):
            self.cnx.close()
            print("MySQL connection is closed")
