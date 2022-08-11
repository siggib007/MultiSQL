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


def Conn (strDBType,strServer,strDBUser,strDBPWD,strInitialDB):
  """
  Function that handles establishing a connection to a specified database
  imports the right module depending on database type
  Parameters:
    strDBType   : The type of database server to connect to
                  Supported server types are mssql, mysql and postgres
    strServer   : Hostname for the database server
    strDBUser   : Database username
    strDBPWD    : Password for the database user
    strInitialDB: The name of the database to use
  Returns:
    Connection object to be used by query function, or an error string
  """

  global dboErr
  global dbo

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

def Query (strSQL,db):
  """
  Function that handles executing a SQL query using a predefined connection object
  imports the right module depending on database type
  Parameters:
    strSQL: The query to be executed
    db    : The connection object to use
  Returns:
    NoneType for queries other than select, DBCursor object with the results from the select query
    or error message as a string
  """

  try:
    dbCursor = db.cursor()
    dbCursor.execute(strSQL)
    if strSQL[:6].lower() != "select":
      db.commit()
      return None
    else:
      return dbCursor
  except dboErr.InternalError as err:
    return "Internal Error: unable to execute: {}\n{}\nLength of SQL statement {}\n".format(err,strSQL[:255],len(strSQL))
  except dboErr.ProgrammingError as err:
    return "Programing Error: unable to execute: {}\n{}\nLength of SQL statement {}\n".format(err,strSQL[:255],len(strSQL))
  except dboErr.OperationalError as err:
    return "Programing Error: unable to execute: {}\n{}\nLength of SQL statement {}\n".format(err,strSQL[:255],len(strSQL))
  except dboErr.IntegrityError as err:
    return "Integrity Error: unable to execute: {}\n{}\nLength of SQL statement {}\n".format(err,strSQL[:255],len(strSQL))
  except dboErr.DataError as err:
    return "Data Error: unable to execute: {}\n{}\nLength of SQL statement {}\n".format(err,strSQL[:255],len(strSQL))
  except dboErr.InterfaceError as err:
    return "Interface Error: unable to execute: {}\n{}\nLength of SQL statement {}\n".format(err,strSQL[:255],len(strSQL))
