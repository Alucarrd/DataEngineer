import logging
import sys
from logging import StreamHandler
#from tqdm import tqdm
import configparser
import pandas as pd
import s3fs
import psycopg2
from sqlalchemy import create_engine
import time
import datetime
#constants
IMMIGRATION_CHUNK_SIZE = 500000
WRITE_CHUNK_SIZE = 500
IMMIGRATION_DATA_FILES = [
    'i94_jan16_sub.sas7bdat',
    'i94_jun16_sub.sas7bdat',
    'i94_feb16_sub.sas7bdat',
    'i94_mar16_sub.sas7bdat',
    'i94_apr16_sub.sas7bdat',
    'i94_may16_sub.sas7bdat',
    'i94_jul16_sub.sas7bdat',
    'i94_aug16_sub.sas7bdat',
    'i94_sep16_sub.sas7bdat',
    'i94_oct16_sub.sas7bdat',
    'i94_nov16_sub.sas7bdat',
    'i94_dec16_sub.sas7bdat'
]
test_csv = 'sample.csv'
#sql statements
sql_drop_fact_immigration = 'DROP TABLE IF EXISTS fact_immigration;'

sql_create_fact_immigration = '''CREATE TABLE fact_immigration(
    immigration_id int not null identity(0,1) primary key,
    cicid decimal not null,
    i94yr decimal not null,
    i94mon decimal not null,
    i94cit decimal,
    i94res decimal,
    i94port char(3),
    arrdate decimal,
    i94mode decimal,
    i94addr char(3),
    depdate decimal,
    i94bir decimal,
    i94visa decimal,
    count decimal,
    dtadfile varchar,
    visapost char(3),
    occup char(3),
    entdepa char(1),
    entdepd char(1),
    entdepu char(1),
    matflag char(1),
    biryear decimal,
    dtaddto varchar,
    gender char(1),
    insnum varchar,
    airline char(3),
    admnum varchar, 
    fltno varchar,
    visatype char(3)
    );'''
#configure logging
logger = logging.getLogger("DataEngineering.Capstone.load_fact_immigration")
logging.basicConfig(
    filename='load_fact_immigration.log',
    level=logging.INFO,
    format='%(levelname) -10s %(asctime)s %(module)s at line %(lineno)d: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',

)

logger.addHandler(StreamHandler(sys.stdout))


def refresh_fact_immigration_table(cursor):
    """
    This method will drop and recreate the fact immigration table
    """
    cursor.execute(sql_drop_fact_immigration)
    cursor.execute(sql_create_fact_immigration)
    
    
def populate_fact_immigration_table(my_engine, aws_key, aws_secret, iam_role, s3_bucket):
    """
    This method reads through all the immigraiton sas file one by one, create dataframe
    , and convert to sql insert to be used for inserting into fact_immigration table.

    """
    file_counter = 1
    s3 = s3fs.S3FileSystem(anon=False,key=aws_key, secret=aws_secret)
    for file in IMMIGRATION_DATA_FILES:
        start = time.time()
        logger.info('reading in {}'.format(file))
        print('reading in {}'.format(file))
        my_iterator = pd.read_sas(
            file, 'sas7bdat', encoding='ISO-8859-1', chunksize=IMMIGRATION_CHUNK_SIZE
        )
        print("Took {} time to read file {}".format(time.time() - start, file))
        start = time.time()
        immigration_df = next(my_iterator)
   
        if file_counter == 1:
            columns_set = immigration_df.columns
        
        logger.info('done reading {}; current df shape:{}'.format(file, immigration_df.shape))
        
        logger.info('ready to insert row')
        
        immigration_df[['cicid', 'i94yr', 'i94mon', 'i94cit','arrdate' ,'i94mode', 'depdate', 'i94bir','i94visa', 'count'  ]] = immigration_df[['cicid', 'i94yr', 'i94mon', 'i94cit','arrdate' ,'i94mode', 'depdate', 'i94bir','i94visa', 'count']].fillna(value=0)
        immigration_df=immigration_df.fillna(value='')

        
        filename = '{}/temp.csv'.format(s3_bucket)
       
        with s3.open(filename, 'w') as f:
            immigration_df[columns_set].to_csv(f, index=False, header=False)
        my_data_path = 's3://{}'.format(filename)
        sql = "copy fact_immigration from '{}' iam_role '{}' csv;commit;".format(my_data_path, iam_role)
        print(sql)
        my_engine.execute(sql)
        file_counter = file_counter + 1
       
        
        #for _, row in immigration_df.iterrows():
        #    cur.execute(sql_insert_template, row)
        s3.rm(filename)
        del immigration_df
        print("Took {} time to insert".format(time.time() - start))
        #break
    


def to_copy(data_path, table_name, iamrole, con):
    """
    Test method used to test the COPU command directly
    """
    sql = "copy {} from '{}' iam_role '{}' csv;commit;".format(table_name, data_path, iamrole)
    print(sql)
    con.execute(sql)
   

    
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('dbaccess.cfg')

    engine_connection_string='postgres://{}:{}@{}:{}/{}'.format(config['CLUSTER']['DB_USER'], config['CLUSTER']['DB_PASSWORD'], config['JDBC']['JDBCURL'], config['CLUSTER']['DB_PORT'], config['CLUSTER']['DB_NAME'])               
    connection_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    aws_key = config['AWS']['KEY']
    aws_secret=config['AWS']['SECRET']
    iam_role=config['AWS']['IAM_ROLE']
    bucket_name=config['AWS']['BUCKET_NAME']
    engine = create_engine(engine_connection_string)  # needed for DataFrame.to_sql
    conn = psycopg2.connect(connection_string)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    refresh_fact_immigration_table(cur)
    populate_fact_immigration_table(engine, aws_key, aws_secret, iam_role, bucket_name)
    

    


    