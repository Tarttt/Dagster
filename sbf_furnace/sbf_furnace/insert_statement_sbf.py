import urllib
import sqlalchemy
from sqlalchemy import create_engine, event, text, MetaData, Table, insert as sqlalchemy_insert, select, func, and_
import logging
import os
from datetime import datetime,timedelta
from datetime import date
#%% Datapoint Query
#Delete all entries before a week ago
Cutoff_Date = datetime.now() - timedelta(days=7)
Cutoff_Time =  datetime.now().strftime('%H:%M:%S')
Cutoff_Date = Cutoff_Date.strftime('%H:%M:%S')


#function to get run ID
#function to get furnace_id
def run_id(engine,table3,furnace_name):
    current_date = date.today()
    with engine.connect() as connection:
        #get max datapoint
        maximum = 0
        try:
            max_datapoint = (select(func.max(table3.c.Run_ID)).where(table3.c.Furnace_Name == furnace_name))
            result = connection.execute(max_datapoint).fetchone()
            maximum = result[0] if result[0] is not None else maximum == 0
        except:
            maximum = 0
        return maximum


#function to get furnace_id
def data_point(engine,table,furnace_id):
    current_date = date.today()
    with engine.connect() as connection:
        #get max datapoint
        maximum = 0
        try:
            max_datapoint = select(table.c.Data_Point).where(and_(table.c.Furnace_ID == furnace_id,(table.c.Date)== current_date)).order_by(table.c.Data_Point.desc()).limit(1)
            result = connection.execute(max_datapoint).fetchone()
            maximum = result[0] if result[0] is not None else maximum == 0
        except:
            maximum = 0
        return maximum + 1
#%% Insert Statement
def insert_statement(furnace_name, furnace_id, **kwargs):


    #connection detail
    Driver='{ODBC Driver 17 for SQL Server}'
    Server = 'central.ionstoragesystems.com'
    Database = 'ProdSys_DB'
    Username = os.getenv('prodsys_user')
    Password = os.getenv('prodsys_pass')
    params = 'Driver='+Driver+';Server='+Server+';Database='+Database+';UID='+Username+';PWD='+ Password + ";TrustServerCertificate=yes"
    table_name = 'Furnace_Data_tb'
    #second insert

    table_name3 = 'Batch_Info_tb'
    #Engine Define
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany = True)
    metadata = MetaData()


    #Table
    table = Table(table_name, metadata, autoload_with=engine)

    table3 = Table(table_name3, metadata, autoload_with=engine)

    #Get next batch ID
    run_id_max = run_id(engine,table3,furnace_name)


    kwargs['Run_ID'] = run_id_max
    #Insert
    insert = sqlalchemy_insert(table).values(kwargs)
    with engine.connect() as connection:
       upload = connection.begin()
       try:
           result =  connection.execute(insert)
           upload.commit()
       except Exception as e:
           upload.rollback()
           print("insertion_failed", e)



#%% logs
import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

### Logging
def logging_setup():
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_dir = os.path.join("logs",year,month)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        print("log directory created")
    date_str = datetime.now().strftime("%Y-%m-%d")
    logfile = os.path.join(log_dir, f'SBF_Furnace_Pipeline_{date_str}.log')
    file_handler = logging.FileHandler(logfile)


import time
import random
import string

# Define base62 character set
chars = list(string.digits + string.ascii_uppercase + string.ascii_lowercase)
def base62_encode(x, chars=chars):
    if x == 0:
        return chars[0]

    char_res = []
    base = len(chars)

    while x > 0:
        frac_ii = x % base
        char_res.insert(0, chars[frac_ii])
        x = x // base

    return ''.join(char_res)

def create_uuid(use_date=True, chars=chars):
    if use_date:
        # Use current UTC time in seconds * 10,000
        right_now = int(time.time() * 10000)
        uuid_1 = base62_encode(right_now, chars)
    else:
        uuid_1 = ''.join(random.choices(chars, k=8))

    uuid_2 = ''.join(random.choices(chars, k=4))
    uuid_3 = ''.join(random.choices(chars, k=4))
    uuid_4 = ''.join(random.choices(chars, k=4))
    uuid_5 = ''.join(random.choices(chars, k=12))

    uuid = f"{uuid_1}-{uuid_2}-{uuid_3}-{uuid_4}-{uuid_5}"
    return uuid


from azure.identity import DefaultAzureCredential
import pyodbc


def utility_insert(furnace_id,uuid,insert_statement_message,Type, insert_time):
    server = 'central.ionstoragesystems.com'
    database = 'Utility_DB'
    username = 'mt_Utility'
    password = os.getenv('UTILITY_PASS')

    now = datetime.now()
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

    if furnace_id == "9":
        source = 'SBF-pipeline'
    if furnace_id == "10":
        source = 'SBF-pipeline'
    if furnace_id == "11":
        source = 'SBF-pipeline'

    columns = ['DateTimeUTC','RUNUUID','Source','Message', 'Type','RunTime']
    data = (now,uuid,source,insert_statement_message,'I',insert_time)
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            #create query
            cols = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(data))
            sql = f"INSERT INTO logs_tb ({cols}) VALUES ({placeholders})"


            #Execute query
            cursor.execute(sql,data)
            conn.commit()
            print("Insert successful")

    except Exception as e:
       print(e)
