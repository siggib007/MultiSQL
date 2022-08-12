'''
Module to handle executing a query against multiple types of databases
Such as MySQL/MariaDB, MS SQL, PostgreSQL, etc.
Contains two functions, one for connecting and another for executing queries
Author Siggi Bjarnason Copyright 2022


Uses the following packages
pip install pymysql
pip install pyodbc
pip install psycopg2

'''
import subprocess
import sys
import os


def Conn (*,DBType,Server,DBUser="",DBPWD="",Database=""):
  """
  Function that handles establishing a connection to a specified database
  imports the right module depending on database type
  Parameters:
    DBType : The type of database server to connect to
                Supported server types are sqlite, mssql, mysql and postgres
    Server : Hostname for the database server
    DBUser : Database username
    DBPWD  : Password for the database user
    Database  : The name of the database to use
  Returns:
    Connection object to be used by query function, or an error string
  """

  strDBType = DBType
  strServer = Server
  strDBUser = DBUser
  strDBPWD = DBPWD
  strInitialDB = Database

  global dboErr
  global dbo

  try:
    if strDBType == "sqlite":
      import sqlite3
      from sqlite3 import Error as dboErr
      strVault = strServer
      strVault = strVault.replace("\\", "/")
      if strVault[-1:] == "/":
        strVault = strVault[:-1]
      if strVault[-3:] != ".db":
        strVault += ".db"
      lstPath = os.path.split(strVault)
      if not os.path.exists(lstPath[0]):
        os.makedirs(lstPath[0])
      return sqlite3.connect(strVault)
  except dboErr as err:
    return("SQLite Connection failure {}".format(err))

  try:
    if strDBType == "mssql":
      try:
        import pyodbc as dbo
      except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", 'pyodbc'])
      finally:
        import pyodbc as dbo
      
      import pyodbc as dboErr
      if strDBUser == "":
        strConnect = (" DRIVER={{ODBC Driver 17 for SQL Server}};"
                      " SERVER={};"
                      " DATABASE={};"
                      " Trusted_Connection=yes;".format(strServer,strInitialDB))
      else:
        strConnect = (" DRIVER={{ODBC Driver 17 for SQL Server}};"
                      " SERVER={};"
                      " DATABASE={};"
                      " UID={};"
                      " PWD={};".format(strServer,strInitialDB,strDBUser,strDBPWD))
      return dbo.connect(strConnect)
    elif strDBType == "mysql":
      try:
        import pymysql as dbo
      except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", 'pymysql'])
      finally:
        import pymysql as dbo
      from pymysql import err as dboErr
      return dbo.connect(host=strServer,user=strDBUser,password=strDBPWD,db=strInitialDB)
    elif strDBType == "postgres":
      try:
        import psycopg2 as dbo
      except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", 'psycopg2'])
      finally:
        import psycopg2 as dbo
      import psycopg2 as dboErr
      return dbo.connect(host=strServer, user=strDBUser, password=strDBPWD, database=strInitialDB)
    else:
      return ("Unknown database type: {}".format(strDBType))
  except dboErr.InternalError as err:
    return ("Error: unable to connect: {}".format(err))
  except dboErr.OperationalError as err:
    return ("Operational Error: unable to connect: {}".format(err))
  except dboErr.ProgrammingError as err:
    return ("Programing Error: unable to connect: {}".format(err))
  except dboErr.InterfaceError as err:
    return ("Interface Error: unable to connect: {}".format(err))

def Query (*,SQL,dbConn):
  """
  Function that handles executing a SQL query using a predefined connection object
  imports the right module depending on database type
  Parameters:
    SQL    : The query to be executed
    dbConn : The connection object to use
  Returns:
    NoneType for queries other than select, DBCursor object with the results from the select query
    or error message as a string
  """
  strSQL = SQL
  try:
    dbCursor = dbConn.cursor()
    dbCursor.execute(strSQL)
    if strSQL[:6].lower() != "select":
      dbConn.commit()
      return None
    else:
      return dbCursor
  except Exception as err:
    return "Failed to execute query: {}\n{}\nLength of SQL statement {}\n".format(err,strSQL[:255],len(strSQL))
