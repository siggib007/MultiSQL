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
import uuid
import random
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
  strTable = FetchEnv("TABLE")  # "SELECT strkey, strValue FROM tblvault;"

  dbConn = ""
  dbCursor = None

  #strTable = "tblTest"
  dictColumn = {}
  dictColumn["iID"] = ["int", "not null"]
  dictColumn["strKey"] = ["text","not null"]
  dictColumn["strValue"] = ["text", "not null"]
  strMSSQLtext = "varchar(MAX)"
  tStart = time.time()
  lstDBTypes = ["mssql","mysql","postgres"]

  dictValues = {}
  for strCol in dictColumn:
    if dictColumn[strCol][0] == "text":
      dictValues[strCol] = "'{}'".format(str(uuid.uuid4()))
    elif dictColumn[strCol][0] == "int":
      dictValues[strCol] = str(random.randint(10, 50))
    else:
      print("Type {} is unexpected".format(dictColumn[strCol][0]))

  for strDBType in lstDBTypes:
    iLineNum = 0
    dbConn = MultiSQL.Conn(DBType=strDBType, Server=strServer,
                         DBUser=strDBUser, DBPWD=strDBPWD, Database=strInitialDB)
    strTableCreate = "CREATE TABLE "
    if strDBType != "mssql":
      strTableCreate += "IF NOT EXISTS "
    strTableCreate += strTable +"("
    for strCol in dictColumn:
      if strDBType == "mssql" and dictColumn[strCol][0] == "text":
        strColType = strMSSQLtext
      else:
        strColType = " ".join(dictColumn[strCol])
      strTableCreate += "{} {}, ".format(strCol,strColType)
    strTableCreate = strTableCreate[:-2] + ");"
    print(strTableCreate)
    strDataInsert = "INSERT INTO {}({}) VALUES({});".format(
        strTable, ",".join(dictColumn.keys()), ",".join(dictValues.values()))
    print(strDataInsert)
    lstKeys = list(dictColumn.keys())
    lstValues = list(dictValues.values())
    strDataUpdate = "UPDATE {} SET {} = {}, {} = {} WHERE {} = {}".format(
        strTable, lstKeys[1], lstValues[2], lstKeys[2], lstValues[1], lstKeys[0], lstValues[0])
    print(strDataUpdate)
    strDataDelete = "DELETE FROM {} WHERE {} = {}".format(
        strTable, lstKeys[0], lstValues[0])
    print(strDataDelete)
    strDataSelect = "SELECT {} FROM {};".format(
        ", ".join(dictColumn.keys()), strTable)
    print(strDataSelect)

    if dbConn is not None:
      LogEntry("{} Database connection established to DB {}".format(
          strDBType, strInitialDB))
      LogEntry("Executing the query : {}".format(strTableCreate))
      if strDBType == "mssql":
        strSQL = "select OBJECT_ID('{}', 'U')".format(strTable)
        dbCursor = MultiSQL.Query(SQL=strSQL, dbConn=dbConn)
        strReturn = dbCursor.fetchone()
        if strReturn[0] is None:
          dbCursor = MultiSQL.Query(SQL=strTableCreate, dbConn=dbConn)
          print("Query complete.")
          if isinstance(dbCursor, str):
            LogEntry("Results is only the following string: {}".format(dbCursor), True)
        else:
          print("Table already exists")
      else:
        dbCursor = MultiSQL.Query(SQL=strTableCreate, dbConn=dbConn)

      print("Now Executing {}".format(strDataInsert))
      dbCursor = MultiSQL.Query(SQL=strDataInsert, dbConn=dbConn)
      print("Query complete.")
      if isinstance(dbCursor, str):
        LogEntry("Results is only the following string: {}".format(dbCursor), True)
      print("Now Executing {}".format(strDataUpdate))
      dbCursor = MultiSQL.Query(SQL=strDataUpdate, dbConn=dbConn)
      print("Query complete.")
      if isinstance(dbCursor, str):
        LogEntry("Results is only the following string: {}".format(dbCursor), True)
      print("Now Executing {}".format(strDataDelete))
      dbCursor = MultiSQL.Query(SQL=strDataDelete, dbConn=dbConn)
      print("Query complete.")
      if isinstance(dbCursor, str):
        LogEntry("Results is only the following string: {}".format(dbCursor), True)
      print("Now Executing {}".format(strDataInsert))
      dbCursor = MultiSQL.Query(SQL=strDataInsert, dbConn=dbConn)
      print("Query complete.")
      if isinstance(dbCursor, str):
        LogEntry("Results is only the following string: {}".format(dbCursor), True)
      print("Now Executing {}".format(strDataSelect))
      dbCursor = MultiSQL.Query(SQL=strDataSelect, dbConn=dbConn)
      print("Query complete.")
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
            strLine = ""
            for field in dbRow:
              strLine += str(field) + " | "
            print(strLine[:-3])
            iLineNum += 1

          print("\nFetched {} lines.\n".format(iLineNum))
      else:
        print("Query exected successfully")
    if dbConn is not None:
      dbConn.close()

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





if __name__ == '__main__':
    main()
