from collections import defaultdict

class dbManager():
    
    _instance = None
    
    def __init__(self):
        self.databases = defaultdict()
    
    @classmethod
    def get_instance(cls):
        
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance        
    
    def nameAvailable(self,dbName):
        if dbName in self.databases:
            return False
        return True
    
    def addDatabase(self,name,dbObject):
        self.databases[name] = dbObject
        
    def getDatabase(self,name):
        return self.databases.get(name,None)
            

class database:
    def __init__(self,name):
        self.name = name
        self.tables = defaultdict()
    def tableNameExist(self,name):
        if name in self.tables:
            return False
        return True
    
    def addTable(self,name,tableObject):
        self.tables[name] = tableObject
        return "Table added"
    
    def getTableObj(self,tableName):
        return self.tables.get(tableName,None)
        

class tables:
    def __init__(self,name):
        self.name =name
        self.columns = defaultdict()
        self.values = []
        self.noOfColumns = 0
        self.itr = 0
    def createTable(self,columns):
        counter = 0
        for columnName, dataType in columns:
            self.columns[columnName] = (counter,dataType)
            counter+=1
        self.noOfColumns = counter+1
    
    def insertIntoTable(self,listOfColumns,listOfValues):
        if len(self.columns)==0:
            return "Create Table First"
        if not len(listOfColumns) == len(listOfValues):
            return "Column Values mismatch"
        if len(listOfValues)>self.noOfColumns:
            return "Extra Values"
        to_insert = [None]*self.noOfColumns
        for itr,cValue in enumerate(listOfColumns):
            position = self.columns.get(cValue,None)
            if position == None:
                return "Invalid Column Specified"
            to_insert[position] = listOfValues[itr]
        self.values.append(to_insert)
        self.itr+=1
    
    def selectData(self,listOfColumns,listOfValues):
        if len(self.columns)==0:
            return "Create Table First"
        if not len(listOfColumns) == len(listOfValues):
            return "Column Values mismatch"
        if len(listOfValues)>self.noOfColumns:
            return "Extra Values"
        positions = [(num,self.columns.get(itr)) for num,itr in enumerate(listOfColumns)]
        found_Data = []
        for rows in self.values:
            flag = True
            for num,pos in positions:
                if rows[pos] == listOfValues[num]:
                    continue
                else:
                    flag = False
                    break
            if(flag):
                found_Data.append(rows)
        return found_Data
    
    def deleteData(self,listOfColumns,listOfValues):
        if len(self.columns)==0:
            return "Create Table First"
        if not len(listOfColumns) == len(listOfValues):
            return "Column Values mismatch"
        if len(listOfValues)>self.noOfColumns:
            return "Extra Values"
        positions = [(num,self.columns.get(itr)) for num,itr in enumerate(listOfColumns)]
        for itr,rows in enumerate(self.values):
            flag = True
            for num,pos in positions:
                if rows[pos] == listOfValues[num]:
                    continue
                else:
                    flag = False
                    break
            if(flag):
                self.values.pop(itr)
        
    def updateData(self,listOfColumns,listOfValues,columnUpdate,columnUpdateValue):
        if len(self.columns)==0:
            return "Create Table First"
        if not len(listOfColumns) == len(listOfValues):
            return "Column Values mismatch"
        if len(listOfValues)>self.noOfColumns:
            return "Extra Values"
        positions = [(num,self.columns.get(itr)) for num,itr in enumerate(listOfColumns)]
        found_Data = []
        updatePositions = [(num,self.columns.get(itr)) for num,itr in enumerate(columnUpdate)]
        counter = 0
        for rows in self.values:
            flag = True
            for num,pos in positions:
                if rows[pos] == listOfValues[num]:
                    continue
                else:
                    flag = False
                    break
            if(flag):
                counter+=1
                for num,uPos in updatePositions:
                    rows[uPos] = columnUpdateValue[num]
        return "%s rows updated successfully" %counter
            

        
    
#Create database

def createDatabase(dbName):
    dbmgr = dbManager.get_instance()
    if dbmgr.nameAvailable(dbName):
        dbObj = database(dbName)
        dbmgr.addDatabase(dbName,dbObj)
        return "Database %s created successfully" %(dbName)
    else:
        return "Database with name exist"


def createTable(useDB,tableName,columnNames):
    dbmgr = dbManager.get_instance()
    currentDB = dbmgr.getDatabase(useDB)
    if not currentDB==None:
        if currentDB.tableNameExist(tableName):
            return "Table with this name already exist"
        else:
            tableObj = createTable(tableName)
            tableObj.createTable(columnNames)
            currentDB.addTable(tableName,tableObj)
    else:
        "No such Database exist"

def insert_into_table(useDB,tableName,list_of_columns,list_of_values):
    dbmgr = dbManager.get_instance()
    currentDB = dbmgr.getDatabase(useDB)
    if not currentDB==None:
        tableObj = currentDB.getTableObj(tableName)
        if tableObj==None:
            "No such table exist"
        else:
            tableObj.insertIntoTable(list_of_columns,list_of_values)
    else:
        "No such Database exist"

def update_table(useDB,tableName,list_of_columns,list_of_values,list_of_column_2be_updated,list_of_values_2be_updated):
    dbmgr = dbManager.get_instance()
    currentDB = dbmgr.getDatabase(useDB)
    if not currentDB==None:
        tableObj = currentDB.getTableObj(tableName)
        if tableObj==None:
            "No such table exist"
        else:
            tableObj.updateData(list_of_columns,list_of_values,list_of_column_2be_updated,list_of_values_2be_updated)
    else:
        "No such Database exist"

def select_data_from_table(useDB,tableName,list_of_columns,list_of_values):
    dbmgr = dbManager.get_instance()
    currentDB = dbmgr.getDatabase(useDB)
    if not currentDB==None:
        tableObj = currentDB.getTableObj(tableName)
        if tableObj==None:
            "No such table exist"
        else:
            tableObj.selectData(list_of_columns,list_of_values)
    else:
        "No such Database exist"

def delete_data_from_table(useDB,tableName,list_of_columns,list_of_values):
    dbmgr = dbManager.get_instance()
    currentDB = dbmgr.getDatabase(useDB)
    if not currentDB==None:
        tableObj = currentDB.getTableObj(tableName)
        if tableObj==None:
            "No such table exist"
        else:
            tableObj.deleteData(list_of_columns,list_of_values)
    else:
        "No such Database exist"
    
            
         
      
        
        
    
    
    
    
        