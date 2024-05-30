import settings_vendor_load as cfg
import pandas as pd
from sqlalchemy import create_engine

pwd_str =f"Pwd={cfg.password};"
global conn
conn = "DRIVER={ODBC Driver 17 for SQL Server};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;" + pwd_str
global engine
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn)
# Create connection string
# conn_str = f'mssql+pyodbc://{cfg.username}:{cfg.password}@{cfg.server}/{cfg.database}?driver=ODBC+Driver+17+for+SQL+Server'
# Create SQL Server engine
# engine = create_engine(conn_str)

filetype = input('''
Upload ready for:  
''')

setupid = 0

if filetype.find('dolce') > 0:
    setupid = 157
    filecolumns = ['BrandID', 'F0', 'F1','F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11', 'F12']

if filetype.find('alexandermcqueen') > 0:
    setupid = 26
    filecolumns = ['BrandID', 'F0', 'F1','F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11','F12','F13','F14', 'F15', 'F16', 'F17', 'F18', 'F19', 'F20', 'F21','F22','F23']

if filetype.find('gucci') > 0:
    setupid = 229
    filecolumns = ['BrandID', 'F0', 'F1','F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11','F12','F13','F14', 'F15', 'F16', 'F17', 'F18', 'F19', 'F20', 'F21','F22','F23','F24','F25','F26','F27','F28','F29','F30','F31','F32','F33','F34']

if filetype.find('bottega') > 0:
    setupid = 93
    filecolumns = ['BrandID', 'F0', 'F1','F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11','F12','F13','F14', 'F15', 'F16', 'F17', 'F18', 'F19', 'F20', 'F21','F22','F23','F24','F25','F26','F27']
if filetype.find('ysl') > 0:
    setupid = 478
    filecolumns = ['BrandID', 'F0', 'F1','F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11','F12','F13','F14', 'F15', 'F16', 'F17', 'F18', 'F19', 'F20', 'F21','F22','F23','F24','F25','F26','F27']
if filetype.find('balenciaga') > 0:
    setupid = 66
    filecolumns = ['BrandID', 'F0', 'F1','F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11','F12','F13','F14', 'F15', 'F16', 'F17', 'F18', 'F19', 'F20', 'F21','F22','F23','F24','F25','F26','F27']
if filetype.find('versace') > 0:
    setupid = 544
    filecolumns = ['BrandID', 'F0', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11', 'F12', 'F13',
                   'F14', 'F15', 'F16', 'F17', 'F18', 'F19', 'F20']
if filetype.find('ferragamo') > 0:
    setupid = 481
    filecolumns = ['BrandID', 'F0', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7']
if filetype.find('balmain') > 0:
    setupid = 68
    filecolumns = ['BrandID', 'F0', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11', 'F12', 'F13',
                   'F14', 'F15', 'F16']
if filetype.find('fendi') > 0:
    setupid = 201
    filecolumns = ['BrandID', 'F0', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8']
    
if filetype.find('moncler') > 0:
    setupid = 363
    filecolumns = ['BrandID', 'F0', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8','F9']
    
if filetype.find('givenchy') > 0:
    setupid = 227
    filecolumns = ['BrandID', 'F0', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8','F9']
    
if filetype.find('mccartney') > 0:
    setupid = 498
    filecolumns = ['BrandID', 'F0', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8','F9']

if setupid > 0 :

    df = pd.read_csv(filetype, quotechar='"',header=None)
    df.insert(0, 'BrandID', setupid)
    df.columns = filecolumns

    f = df.iloc[2:]
    df.to_sql('utb_RetailLoadInitial', engine, if_exists='append', index=False)

    print ("Upload Completed")
else:
    print ("Setup not found")









