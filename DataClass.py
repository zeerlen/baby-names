import pymongo
import mysql.connector
import json
from MySQLClass import MySQLClass, MySQLEngineTypes
from MongoDBClass import MongoDBClass
from SQLite3Class import SQLite3Class

class DataClass:
    dataInstanceCount = 0

    def __init__(self,
                    description: str,
                    mysqlClassObject: MySQLClass = None,
                    mongoDBObject: MongoDBClass = None,
                    sqlite3Object: SQLite3Class = None):
        DataClass.dataInstanceCount += 1
        self.description = description
        self.mysqlClassObject = mysqlClassObject
        self.mongoDBObject = mongoDBObject
        self.sqlite3Object = sqlite3Object

    def getDescription(self):
        return self.description

    def setDatabaseClassObjects(self,
                                            mysqlClassObject: MySQLClass = None,
                                            mongoDBObject: MongoDBClass = None,
                                            sqlite3Object: SQLite3Class = None):
        self.mysqlClassObject = mysqlClassObject
        self.mongoDBObject = mongoDBObject
        self.sqlite3Object = sqlite3Object

    def setMySQLClassObject(self, mysqlClassObject: MySQLClass=None):
        self.mysqlClassObject = mysqlClassObject

    def setMongoClassObject(self, mongoDBObject: MongoDBClass = None):
        self.mongoDBObject = mongoDBObject

    def setSQLite3ClassClassObject(self, sqlite3Object: SQLite3Class = None):
        self.sqlite3Object = sqlite3Object

    def dataMongoToMySQL(self,
                                        collectionTable: str,
                                        mongoDBCursor: pymongo.cursor.Cursor,
                                        mysqlDBEngineType: str = MySQLEngineTypes.INNODB.value,
                                        bulkInsert: bool = False):
        if self.mongoDBObject == None:
            print("No Valid MongoDBClass Object")
            return False
        if self.mysqlClassObject == None:
            print("No Valid MySQLClass Object")
            return False


        self.mysqlClassObject.mysqlCreateDataBase(self.mongoDBObject.getDataBaseName())

        self.mysqlClassObject.mysqlConnectDataBase(self.mongoDBObject.getDataBaseName())

        self.mysqlClassObject.mySQLCreateDatabaseTableJsonType(collectionTable, mysqlDBEngineType)


        strQuery_1 = "INSERT INTO " + collectionTable + "(json_record) VALUES"
        strQuery_2 = ""
        for records in mongoDBCursor:
            if not bulkInsert:
                strQuery_2 = "(\""+json.dumps(records).replace("\"","\"\"")+"\")"
                self.mysqlClassObject.mysqlExecuteInsert(strQuery_1 + strQuery_2)
            else:
                strQuery_2 += "(\""+json.dumps(records).replace("\"","\"\"")+"\"),"

        if bulkInsert:
            strQuery_2 = strQuery_2[:-1]
            self.mysqlClassObject.mysqlExecuteInsert(strQuery_1 + strQuery_2)

    def dataMySQLToMongoDB(self,
                                        mysqlTableName: str,
                                        mysqlDBCursorResults):
        if self.mongoDBObject == None:
            print("No Valid MongoDBClass Object")
            return False
        if self.mysqlClassObject == None:
            print("No Valid MySQLClass Object")
            return False

        dictEntry = {}
        listDictRecords = []
        columnCount = 0
        columnNames = self.mysqlClassObject.columnNames
        columnLen = len(columnNames)

        for rowRecords in mysqlDBCursorResults:
            while columnCount < columnLen:
                try:
                    dictEntry[str(columnNames[columnCount])] = rowRecords[columnCount].decode("utf-8")
                except:
                    dictEntry[str(columnNames[columnCount])] = str(rowRecords[columnCount])
                
                columnCount += 1
            listDictRecords.append(dictEntry)
            dictEntry = {}
            columnCount = 0

        self.mongoDBObject.mongoConnectDataBase(self.mysqlClassObject.getDataBaseName())
        self.mongoDBObject.mongoInsertManyRecords(mysqlTableName, listDictRecords)

    def __del__(self):
        DataClass.dataInstanceCount -= 1