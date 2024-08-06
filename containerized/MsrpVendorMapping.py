import settings_vendor_load as cfg
import pandas as pd
from sqlalchemy import create_engine,text

pwd_str =f"Pwd={cfg.password};"
global conn
conn = "DRIVER={ODBC Driver 17 for SQL Server};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;" + pwd_str
global engine
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn)

brandID = input('''
enter BrandId:  
''')

def initialize_load(brandID):
    if int(brandID) > 0:
        connection = engine.connect()
        sql = text('Delete from utb_RetailLoadTemp Where BrandID = ' + str(brandID))
        print(sql)
        connection.execute(sql)
        connection.commit()
        connection.close()




# UPDATE utb_RetailLoadInitial
#                     SET F0 = CASE
#                     WHEN CHARINDEX('This', F0) > 0 THEN
#                     LTRIM(RTRIM(SUBSTRING(F0, 1, CHARINDEX('This', F0) - 1)))
#                     ELSE F0
#                     END
#                     WHERE BrandID = 125
def sql_execute(sql):
    if len(sql) > 0:
        connection = engine.connect()
        sql = text(sql)
        print(sql)
        connection.execute(sql)
        connection.commit()
        connection.close()
    else:
        print('sql empty for brandi' + brandID)



initialize_load(brandID)
sql = create_sql(brandID)
sql_execute(sql)
validate_sql = validate_temp_load(brandID)
sql_execute(validate_sql)



