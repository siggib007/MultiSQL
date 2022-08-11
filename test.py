'''
Script to test the MultiSQL module

Author Siggi Bjarnason Copyright 2022


'''
# Import libraries
import MultiSQL
import os
import time
import sys
import platform
# End imports


def FetchEnv(strVarName):
  """
  Function that fetches the specified content of specified environment variable, 
  converting nonetype to empty string.
  Parameters:
    strVarName: The name of the environment variable to be fetched
  Returns:
    The content of the environment or empty string
  """

  if os.getenv(strVarName) != "" and os.getenv(strVarName) is not None:
    return os.getenv(strVarName)
  else:
    return ""

def LogEntry(strMsg, bAbort=False):
  print(strMsg)
  if bAbort:
    print("{} is exiting abnormally on {}".format(
        strScriptName, strScriptHost))
    if dbConn != "":
      dbConn.close()
    sys.exit(9)


def main():
  global strScriptName
  global strScriptHost
  global dbConn

  strScriptName = os.path.basename(sys.argv[0])
  strRealPath = os.path.realpath(sys.argv[0])
  strVersion = "{0}.{1}.{2}".format(
      sys.version_info[0], sys.version_info[1], sys.version_info[2])

  strScriptHost = platform.node().upper()

  print("This is a script to execute SQL command from different kind of databases. This is running under Python Version {}".format(strVersion))
  print("Running from: {}".format(strRealPath))
  now = time.asctime()
  print("The time now is {}".format(now))

  strServer = FetchEnv("HOST")
  strInitialDB = FetchEnv("DB")
  strDBPWD = FetchEnv("DBPWD")
  strDBUser = FetchEnv("DBUSSER")
  strDBType = FetchEnv("STORE")
  strSQL = FetchEnv("SQL")  # "SELECT strkey, strValue FROM tblvault;"

  dbConn = ""
  dbCursor = None
  iLineNum = 1

  #strInitialDB = "test"
  strTable = "tblVault"
  dictColumn = {}
  dictColumn["strKey"] = "text"
  dictColumn["strValue"] = "text"
  strMSSQLtext = "varchar(MAX)"
  strTableCreate = "CREATE TABLE "
  if strDBType != "mssql":
    strTableCreate += "IF NOT EXISTS "
  strTableCreate += strTable +"("
  for strCol in dictColumn:
    strTableCreate += "{} {} not null,"


  strSQLSelect = "select * from tblVault"
  strSQLInsert = "INSERT INTO tblvault (strkey,strValue) VALUES ('test15','asdfljalskdjfalj');"
  strSQLUpdate = "UPDATE tblvault SET strValue = 'my testing stuff' WHERE strKey = 'test15';"

  strSQL = strSQLInsert
  tStart = time.time()
  dbConn = MultiSQL.Conn(DBType=strDBType, Server=strServer, DBUser=strDBUser, DBPWD=strDBPWD, Database=strInitialDB)
  if dbConn is not None:
    LogEntry("{} Database connection established to DB {}, executing the query : {}".format(
        strDBType, strInitialDB, strSQL))
    if strDBType == "mssql" and strSQL[:21] == "CREATE TABLE tblVault":
      strSQL2 = "select OBJECT_ID('tblVault', 'U')"
      dbCursor = MultiSQL.Query(strSQL2, dbConn)
      strReturn = dbCursor.fetchone()
      if strReturn[0] is None:
        dbCursor = MultiSQL.Query(SQL=strSQL, dbConn=dbConn)
      else:
        print("Table already exists")
    else:
      dbCursor = MultiSQL.Query(SQL=strSQL, dbConn=dbConn)
    print("Query complete. cursor: {} \n".format(type(dbCursor)))
    if isinstance(dbCursor, str):
      LogEntry("Results is only the following string: {}".format(dbCursor), True)

  strSQL = strSQLUpdate
  dbCursor = MultiSQL.Query(SQL=strSQL, dbConn=dbConn)
  print("Query complete. cursor: {} \n".format(type(dbCursor)))
  if isinstance(dbCursor, str):
    LogEntry("Results is only the following string: {}".format(dbCursor), True)

  strSQL = strSQLSelect
  dbCursor = MultiSQL.Query(SQL=strSQL, dbConn=dbConn)
  print("Query complete. cursor: {} \n".format(type(dbCursor)))
  if isinstance(dbCursor, str):
    LogEntry("Results is only the following string: {}".format(dbCursor), True)

  if dbCursor is not None:
    lstHeader = []
    if dbCursor.description is not None:
      for temp in dbCursor.description:
        lstHeader.append(temp[0])
      print(" | ".join(lstHeader))
      print("="*120)

      for dbRow in dbCursor:
        print(" | ".join(dbRow))
        iLineNum += 1

      print("\nFetched {} lines.\n".format(iLineNum))
  else:
    print("Query exected successfully")

  tStop = time.time()
  iElapseSec = tStop - tStart
  iMin, iSec = divmod(iElapseSec, 60)
  iHours, iMin = divmod(iMin, 60)

  now = time.asctime()
  LogEntry("Completed at {}".format(now))
  LogEntry("Took {0:.2f} seconds to complete, which is {1} hours, {2} minutes and {3:.2f} seconds.".format(
      iElapseSec, iHours, iMin, iSec))
  print("{} completed successfully on {}".format(
      strScriptName, strScriptHost))
  if dbConn is not None:
    dbConn.close()


if __name__ == '__main__':
    main()
