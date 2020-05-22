import sqlite3

class SQLiteInspector():
   
    def __init__(self, name):
        self.name = name
        self.conn = sqlite3.connect(self.name)
        self.cursor = self.conn.cursor()         

    def closeDB(self):
        self.conn.close()

    def codeTypical(self, string):
        self.cursor.execute(string)
        self.conn.commit()

    def createTable(self, string):
        self.codeTypical(string)

    def dropTable(self, string):
        self.codeTypical(string)

    def alterTable(self, string):
        self.codeTypical(string)
    
    def insertValue(self, string):
        self.codeTypical(string)

    def deleteValue(self, string):
        self.codeTypical(string)

    def updateValue(self, string):
        self.codeTypical(string)

    def selectValues(self, string):
        self.cursor.execute(string)
        return self.cursor.fetchall()
