import pyodbc 
import sqlalchemy as alch
import sys
import pandas as pd
import urllib

class DbConnection():


    DRIVER = r'Driver={SQL Server};'
    SERVER = r'Server=DESKTOP-DDD4P5R\SQLEXPRESS;'
    DATABASE = r'Database=AdventureWorks2022;'
    TRUSTED = r'Trusted_Connection=yes;' 

    alchemy = False
    conn = ''
    cursor = ''


    def __init__(self, alchemy: bool):

        if alchemy:
            self.connect_sql_alchemy()
        else:
            self.connect_pyodbc()


    def __enter__(self):
        return self


    def __exit__(self, exc_msg=None):
        print(exc_msg)
        if self.cursor:
            self.cursor.close()
        self.conn.close()


    def connect_sql_alchemy(self):
        try:
            params = urllib.parse.quote_plus(self.DRIVER + self.SERVER + self.DATABASE + self.TRUSTED)

            engine = alch.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            self.conn = engine.connect()

            self.alchemy = True
        except Exception:
            self.__exit__(sys.exc_info())


    def connect_pyodbc(self):
        try:
            self.conn = pyodbc.connect(self.DRIVER + self.SERVER + self.DATABASE + self.TRUSTED)
        except Exception:
            self.__exit__(sys.exc_info())

        self.cursor = self.conn.cursor() 


    def executeQuery(self, query):
        try:
            self.cursor.execute(query)
            return self.cursor
        except pyodbc.Error as ex:
            print(ex.args[1])    


    def executeQuery_toDataFrame(self, query):

        data = pd.read_sql(query, self.conn)
        return data
    

    def callStoredProcedure_onlyInputs(self, sp, parameters):
        comma_parameters = self.read_parameters(parameters)

        sql_to_execute = f'''
            EXECUTE {sp}
                {comma_parameters}
        '''
        return self.executeQuery(sql_to_execute)
    

    def read_parameters(self, parameters: tuple):
        sep_parameters = ''
        for item in parameters:
            if isinstance(item, str):
                sep_parameters += "'" + str(item) + "'"
            else:
                sep_parameters += str(item)
            sep_parameters += ','

        return sep_parameters[:-1]


    def callStoredProcedure_readOutputs(self, sp, parameters):
        pass


if __name__ == '__main__':
    db = DbConnection(False)
    query = 'select * from Production.ProductCategory'
    product_categories = db.executeQuery(query)
    # print(product_categories)

    # df_product_categories = db.executeQuery_toDataFrame(query)
    # print(df_product_categories)

    # print(product_categorise)

    for i in product_categories:
        print(i)

    # sp = db.callStoredProcedure_onlyInputs('uspGetEmployeeManagers', (8,))
    #sp = db.callStoredProcedure_onlyInputs('uspGetWhereUsedProductID', (806, '2010-07-29'))
    # print(type(sp))

    # for i in sp:
    #     print(i)

    # row = db.cursor.fetchone()
    # print(row[0])

    db.__exit__('INFO: Connection closed')