import mysql.connector
from mysql.connector import errorcode
from enum import Enum
from DatabaseClass import DatabaseClass

class MySQLEngineTypes(Enum):
    INNODB = "InnoDB"
    MYISAM = "MyISAM"
    MEMORY = "MEMORY"

class MySQLClass(DatabaseClass):
    mysqlDBInstanceCount = 0
    INNODB = "InnoDB"
    MYISAM = "MyISAM"
    MEMORY = "MEMORY"

    def __init__(self,
                host: str,
                userName: str,
                passWord: str,
                auth_plugin: str = 'mysql_native_password'):
        DatabaseClass.__init__(self, "MySQL Database")
        MySQLClass.mysqlDBInstanceCount += 1
        self.mysqlInstanceID = DatabaseClass.instanceSeedID
        self.host = host
        self.userName = userName
        self.passWord = passWord
        self.auth_plugin = auth_plugin

    def mysqlConnect(self):
        try:
            self.dbConnectorConnect = mysql.connector.connect(user=self.userName,
                                                            password=self.passWord,
                                                            host=self.host,
                                                            database='',
                                                            auth_plugin=self.auth_plugin)
        except mysql.connector.Error as errorMsg:
            print(errorMsg)
        
    def mysqlConnectDataBase(self, dataBaseName: str):
        try:
            self.dataBaseName = dataBaseName
            self.dbConnectorConnect = mysql.connector.connect(user=self.userName,
                                                            password=self.passWord,
                                                            host=self.host,
                                                            database=self.dataBaseName,
                                                            auth_plugin=self.auth_plugin)
            return self.dbConnectorConnect.connection_id
        except mysql.connector.Error as errorMsg:
            print(errorMsg)
            exit(1)

    def mysqlCreateDataBase(self, dataBaseName: str):
        try:
            self.dataBaseName = dataBaseName
            self.cursor = self.dbConnectorConnect.cursor()
            self.cursor.execute("CREATE DATABASE "+dataBaseName)
            self.cursor.close()
        except mysql.connector.Error as errorMsg:
            print(errorMsg)
            if errorMsg.errno != errorcode.ER_DB_CREATE_EXISTS:
                exit(1)

    def mysqlCreateDatabaseTable(self,
                                databaseTableName: str,
                                columnNames: str = "",
                                mysqlDBEngineType: str = MySQLEngineTypes.INNODB.value):
        try:
            self.cursor = self.dbConnectorConnect.cursor()
            self.cursor.execute("CREATE TABLE "+databaseTableName+" "+columnNames)
        except mysql.connector.Error as errorMsg:
            print(errorMsg)
            if errorMsg.errno != errorcode.ER_TABLE_EXISTS_ERROR:
                exit(1)

    def mySQLCreateDatabaseTableJsonType(self,
                                        databaseTableName: str,
                                        mysqlDBEngineType: str = MySQLEngineTypes.INNODB.value):
        try:
            print(mysqlDBEngineType)
            self.cursor = self.dbConnectorConnect.cursor()
            self.cursor.execute("CREATE TABLE "
                                + databaseTableName +
                                " (id INT NOT NULL AUTO_INCREMENT, json_record JSON, PRIMARY KEY (id)) ENGINE=" + mysqlDBEngineType + ";")
        except  mysql.connector.Error as errorMsg:
            print(errorMsg)
            if errorMsg.errno != errorcode.ER_TABLE_EXISTS_ERROR:
                exit(1)

    def mysqlExecuteQuery(self, strSQL: str):
        try:
            self.cursor = self.dbConnectorConnect.cursor()
            self.cursor.execute(strSQL)
            result = self.cursor.fetchall()
            self.columnNames = self.cursor.column_names
            self.cursor.close()
            return result
        except mysql.connector.Error as errorMsg:
            print(errorMsg)
            exit(1)

    def mysqlExecuteInsert(self, strSQL: str):
        try:
            self.cursor = self.dbConnectorConnect.cursor()
            self.cursor.execute(strSQL)
            self.dbConnectorConnect.commit()
            self.cursor.close()
        except mysql.connector.Error as errorMsg:
            print(errorMsg)
            exit(1)

    def mysqlExecuteInsertUsingValues(self, strSQL: str, strValues: str):
        try:
            self.cursor = self.dbConnectorConnect.cursor()
            self.cursor.execute(strSQL, strValues)
            self.dbConnectorConnect.commit()
            self.cursor.close()
        except mysql.connector.Error as errorMsg:
            print(errorMsg)
            exit(1)

    def getMySqlShowDatabases(self):
        try:
            self.cursor = self.dbConnectorConnect.cursor()
            self.cursor.execute("SHOW DATABASES")
            for dbs in self.cursor:
                print(dbs)
            self.cursor.close()
        except mysql.connector.Error as errorMsg:
            print(errorMsg)
            exit(1)
        
    def getMySqlClassInstanceCount(self):
        return MySQLClass.mysqlDBInstanceCount
    
    def getMySqlClassInstanceID(self):
        return self.mysqlInstanceID
    
    def __del__(self):
        MySQLClass.mysqlDBInstanceCount -= 1
        DatabaseClass.__del__(self, self.description, self.mysqlInstanceID)