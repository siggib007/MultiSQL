'''
Module to handle executing a query against multiple types of databases
Such as MySQL/MariaDB, MS SQL, PostgreSQL, etc.
Contains two functions, one for connecting and another for executing queries
Author Siggi Bjarnason Copyright 2022


Uses the following packages, which CheckDependency will try to install if missing
pip install pymysql
pip install pyodbc
pip install psycopg2

'''
import subprocess
import sys
import os

def CheckDependency(Module):
  """
  Function that installs missing depedencies
  Parameters:
    Module : The name of the module that should be installed
  Returns:
    Integer 0 for "all good", non-zero for failure
  """
  lstOutput = subprocess.run(
      [sys.executable, "-m", "pip", "list"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  dictComponents = {}
  lstLines = lstOutput.stdout.decode("utf-8").splitlines()
  for strLine in lstLines:
    lstParts = strLine.split()
    dictComponents[lstParts[0].lower()] = lstParts[1]
  strModule = Module
  if strModule.lower() not in dictComponents:
    lstOutput = subprocess.run(
        [sys.executable, "-m", "pip", "install", strModule], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return lstOutput.returncode
  else:
    return 0


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

  if strServer == "":
    return "Servername can't be empty"

  try:
    if strDBType == "sqlite":
      import sqlite3
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
      if CheckDependency("pyodbc") != 0:
        return "failed to install pyodbc. Please pip install pyodbc before using MS SQL option."
      import pyodbc as dbo
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
      if CheckDependency("pymysql") != 0:
        return "failed to install pymysql. Please pip install pymysql before using mySQL option."
      import pymysql as dbo
      from pymysql import err as dboErr
      return dbo.connect(host=strServer,user=strDBUser,password=strDBPWD,db=strInitialDB)
    elif strDBType == "postgres":
      if CheckDependency("psycopg2-binary") != 0:
        return "failed to install psycopg2-binary. Please pip install psycopg2-binary before using PostgreSQL option."
      import psycopg2 as dbo
      return dbo.connect(host=strServer, user=strDBUser, password=strDBPWD, database=strInitialDB)
    else:
      return ("Unknown database type: {}".format(strDBType))
  except Exception as err:
    return ("Error: unable to connect: {}".format(err))

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
