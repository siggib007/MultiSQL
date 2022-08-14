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
    dictionary object without output from the installation.
      if the module needed to be installed
        code: Return code from the installation
        stdout: output from the installation
        stderr: errors from the installation
        args: list object with the arguments used during installation
        success: true/false boolean indicating success.
      if module was already installed so no action was taken
        code: -5
        stdout: Simple String: {module} version {x.y.z} already installed
        stderr: Nonetype
        args: module name as passed in
        success: True as a boolean
  """
  dictComponents = {}
  dictReturn = {}
  strModule = Module
  lstOutput = subprocess.run(
      [sys.executable, "-m", "pip", "list"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  lstLines = lstOutput.stdout.decode("utf-8").splitlines()
  for strLine in lstLines:
    lstParts = strLine.split()
    dictComponents[lstParts[0].lower()] = lstParts[1]
  if strModule.lower() not in dictComponents:
    lstOutput = subprocess.run(
        [sys.executable, "-m", "pip", "install", strModule], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    dictReturn["code"] = lstOutput.returncode
    dictReturn["stdout"] = lstOutput.stdout.decode("utf-8")
    dictReturn["stderr"] = lstOutput.stderr.decode("utf-8")
    dictReturn["args"] = lstOutput.args
    if lstOutput.returncode == 0:
      dictReturn["success"] = True
    else:
      dictReturn["success"] = False
    return dictReturn
  else:
    dictReturn["code"] = -5
    dictReturn["stdout"] = "{} version {} already installed".format(
        strModule, dictComponents[strModule.lower()])
    dictReturn["stderr"] = None
    dictReturn["args"] = strModule
    dictReturn["success"] = True
    return dictReturn


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
      if not CheckDependency("pyodbc")["success"]:
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
      if not CheckDependency("pymysql")["success"]:
        return "failed to install pymysql. Please pip install pymysql before using mySQL option."
      import pymysql as dbo
      from pymysql import err as dboErr
      return dbo.connect(host=strServer,user=strDBUser,password=strDBPWD,db=strInitialDB)
    elif strDBType == "postgres":
      if not CheckDependency("psycopg2-binary")["success"]:
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
