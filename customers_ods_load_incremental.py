#import requests
from datetime import datetime
import pandas as pd
import json
import time
from google.cloud import storage
from pandas.io.json import json_normalize
from google.cloud import bigquery
import os
import psycopg2
import configparser as cp
import logging
from concurrent.futures import ThreadPoolExecutor
from LogDBHandler import LogDBHandler
import set_environ_vars
import sys

def get_batch_list():
    #get all blobs listed:
    success=0
    try_count=1
    while success==0 and try_count<=5:
        try:
            blob_list = list(BUCKET.list_blobs(prefix=BATCH_PREFIX))
            success = 1
        except Exception as e:
            if try_count==5:  #on last try iteration exit function with empty list
                db_logger.error('Get Batch List Failed! See details in the log file %s' % LOG_FILE)
                f_logger.error('Get Batch List Failed!', exc_info=True, stack_info=True)
                return []
            time.sleep(1+try_count)
            try_count +=1
    db_logger.info('Customers blob count: %s' % len(blob_list))
    #compose list of batch lists:
    batch_file_list = [{"batch": y//BATCH_SIZE, "file": bl.name} for y,bl in enumerate(blob_list) if y//BATCH_SIZE < BATCH_LIMIT]
    df_batch = pd.DataFrame(batch_file_list, columns = ['batch','file'])
    df = df_batch.groupby('batch').agg(list).reset_index()
    del df_batch
    db_logger.info('Number of batches: {}'.format(df.shape[0]))
    #convert batch lists into strings so that will serve as iterator for multiprocessing!!!!:
    b_list = list(df['file'])
    batch_list = ['|'.join(b) for b in b_list]

    return batch_list

#processing function:
def load_json_bq(batch):
    #print(datetime.utcnow())
    #print(batch)
    json_data=[]
    for f in batch.split('|'): #this is what would happen for each file in a batch!!!!
        success=0
        try_count=1
        while success==0 and try_count<=5:
            try:
                blob = BUCKET.get_blob(f)
                json_data.extend(json.loads(blob.download_as_string()))
                success = 1
            except Exception as e:
                if try_count==5:  #on last try iteration exit function with empty list
                    db_logger.error('Batch Failed: {}'.format(batch))
                    f_logger.error('Batch Failed: {}'.format(batch), exc_info=True, stack_info=True)
                    return
                time.sleep(1+try_count)
                try_count +=1
    #get batch timestamp:
    batch_dt = datetime.now()

    #Load to BQ:
    #customers table:
    table_name = 'customers'
    table_columns = ['accepts_marketing'
    ,'admin_graphql_api_id'
    ,'created_at'
    ,'currency'
    ,'email'
    ,'first_name'
    ,'id'
    ,'last_name'
    ,'last_order_id'
    ,'last_order_name'
    ,'multipass_identifier'
    ,'note'
    ,'orders_count'
    ,'phone'
    ,'state'
    ,'tags'
    ,'tax_exempt'
    ,'total_spent'
    ,'updated_at'
    ,'verified_email']
    df=json_normalize(json_data)
    if not df.empty:
        #format column names:
        df.columns = [x.strip().replace('.', '_') for x in df.columns]
        #only selected columns:
        df_schema = pd.DataFrame(df, columns=table_columns)#df.loc[:,table_columns]
        df_schema['ods_inserted_at'] = batch_dt
        del df
        #convert datatypes:
        df_schema['accepts_marketing'] = df_schema['accepts_marketing'].astype('O')
        df_schema['admin_graphql_api_id'] = df_schema['admin_graphql_api_id'].astype('O')
        df_schema['currency'] = df_schema['currency'].astype('O')
        df_schema['email'] = df_schema['email'].astype('O')
        df_schema['first_name'] = df_schema['first_name'].astype('O')
        df_schema['last_name'] = df_schema['last_name'].astype('O')
        df_schema['last_order_name'] = df_schema['last_order_name'].astype('O')
        df_schema['multipass_identifier'] = df_schema['multipass_identifier'].astype('O')
        df_schema['note'] = df_schema['note'].astype('O')
        df_schema['phone'] = df_schema['phone'].astype('O')
        df_schema['state'] = df_schema['state'].astype('O')
        df_schema['tags'] = df_schema['tags'].astype('O')
        df_schema['tax_exempt'] = df_schema['tax_exempt'].astype('O')
        df_schema['verified_email'] = df_schema['verified_email'].astype('O')
        df_schema['id'] = df_schema['id'].astype('int')
        df_schema['created_at'] = pd.to_datetime(df_schema['created_at'])
        df_schema['updated_at'] = pd.to_datetime(df_schema['updated_at'])
        df_schema['orders_count'] = df_schema['orders_count'].fillna(0).astype('int')
        df_schema['last_order_id'] = df_schema['last_order_id'].fillna(0).astype('int')
        df_schema['total_spent'] = df_schema['total_spent'].fillna(0).astype('float64')


        success=0
        try_count=1
        while success==0 and try_count<=5:
            try:
            #upload data to table:
                df_schema.to_gbq('{}.{}'.format(DATASET_ID, table_name),
                                PROJECT_ID,
                                chunksize=None,
                                if_exists='append',
                                private_key=SERVICE_ACCOUNT_KEY_FILE
                                )
                success = 1
            except Exception as e:
                if try_count==5:  #on last try iteration exit function with empty list
                    db_logger.error('Table {} load failed: batch - {}'.format(table_name,batch))
                    f_logger.error('Table {} load failed: batch - {}'.format(table_name,batch), exc_info=True, stack_info=True)
                    return
                time.sleep(1+try_count)
                try_count +=1

    #customer_address table:
    table_name = 'customer_address'
    table_columns = ['id'
    ,'customer_id'
    ,'customer_updated_at'
    ,'first_name'
    ,'last_name'
    ,'address1'
    ,'address2'
    ,'city'
    ,'company'
    ,'country'
    ,'country_code'
    ,'country_name'
    ,'province'
    ,'province_code'
    ,'zip'
    ,'phone'
    ,'name'
    ,'default']
    df=json_normalize(json_data,'addresses', ['updated_at'], meta_prefix='customer_')

    if not df.empty:
        #format column names:
        df.columns = [x.strip().replace('.', '_') for x in df.columns]
        #only selected columns:
        df_schema = pd.DataFrame(df, columns=table_columns)#df.loc[:,table_columns]
        df_schema['ods_inserted_at'] = batch_dt
        del df
        #convert column datatypes:
        df_schema['first_name'] = df_schema['first_name'].astype('O')
        df_schema['last_name'] = df_schema['last_name'].astype('O')
        df_schema['address1'] = df_schema['address1'].astype('O')
        df_schema['address2'] = df_schema['address2'].astype('O')
        df_schema['city'] = df_schema['city'].astype('O')
        df_schema['company'] = df_schema['company'].astype('O')
        df_schema['country'] = df_schema['country'].astype('O')
        df_schema['country_code'] = df_schema['country_code'].astype('O')
        df_schema['country_name'] = df_schema['country_name'].astype('O')
        df_schema['province'] = df_schema['province'].astype('O')
        df_schema['province_code'] = df_schema['province_code'].astype('O')
        df_schema['zip'] = df_schema['zip'].astype('O')
        df_schema['phone'] = df_schema['phone'].astype('O')
        df_schema['name'] = df_schema['name'].astype('O')
        df_schema['id'] = df_schema['id'].astype('int')
        df_schema['customer_id'] = df_schema['customer_id'].astype('int')
        df_schema['customer_updated_at'] = pd.to_datetime(df_schema['customer_updated_at'])
        df_schema['default'] = df_schema['default'].astype('bool')

        success=0
        try_count=1
        while success==0 and try_count<=5:
            try:
            #upload data to table:
                df_schema.to_gbq('{}.{}'.format(DATASET_ID, table_name),
                                PROJECT_ID,
                                chunksize=None,
                                if_exists='append',
                                private_key=SERVICE_ACCOUNT_KEY_FILE
                                )
                success = 1
            except Exception as e:
                if try_count==5:  #on last try iteration exit function with empty list
                    db_logger.error('Table {} load failed: batch - {}'.format(table_name,batch))
                    f_logger.error('Table {} load failed: batch - {}'.format(table_name,batch), exc_info=True, stack_info=True)
                    return
                time.sleep(1+try_count)
                try_count +=1
    #print('{} loaded'.format(table_name))
    #rename processed files:
    for f in batch.split('|'): #this is what would happen for each file in a batch!!!!
        new_name = '{}/{}'.format(PROCCESSED_PREFIX,f.split('/')[-1])
        success=0
        try_count=1
        while success==0 and try_count<=5:
            try:
                blob = BUCKET.get_blob(f)
                BUCKET.rename_blob(blob, new_name, client=CLIENT)
                success = 1
            except Exception as e:
                if try_count==5:  #on last try iteration exit function with empty list
                    db_logger.error('Blob {} rename failed: batch - {}, '.format(f,batch))
                    f_logger.error('Blob {} rename failed: batch - {}, '.format(f,batch), exc_info=True, stack_info=True)
                    return
                time.sleep(1+try_count)
                try_count +=1
#execution starts here:
#SCENARIO = ''
#BATCH_SIZE = 0
#DAG_ID = ''
def main():
    global SCENARIO
    global BATCH_SIZE
    global DAG_ID
    if len(sys.argv) == 4:
        SCENARIO = sys.argv[1]
        BATCH_SIZE = int(sys.argv[2])
        DAG_ID = sys.argv[3]
    else:
        sys.exit()

if __name__ == '__main__':
    # sys.exit(main(sys.argv)) # used to give a better look to exists
    main()
    #set up configs:
    #DAG_ID = 'shopify_orders_incremental_{}'.format(SCENARIO)
    SCRIPT_NAME =  os.path.basename(__file__)
    CONFIG_PATH = os.environ['CONFIG_PATH']
    config = cp.ConfigParser()
    config.read(CONFIG_PATH + 'shopify.ini')
    #load configs:
    SOURCE='shopify'
    API = 'customers'
    PROCESS_TARGET = 'ods'
    LOAD_TYPE = 'incremental'
    #gcp configs:
    SERVICE_ACCOUNT_KEY_FILE = config['gcp']['service_account_key_file']
    BUCKET_NAME = config['gcp']['bucket_name']
    CLIENT = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_FILE)
    BUCKET = CLIENT.get_bucket(BUCKET_NAME)

    #BigQuery configs:
    DATASET_ID = config['gcp']['dataset_id']
    PROJECT_ID = config['gcp']['project_id']

    BATCH_LIMIT = 1000  #because of BQ limit on number of daily table updates
    if SCENARIO == 'hourly':
        BATCH_PREFIX = 'shopify/customers/incremental'
        PROCCESSED_PREFIX = 'shopify/customers/ods/incremental'
    if SCENARIO == 'backfill':
        BATCH_PREFIX = '{}/{}/{}'.format(SOURCE,API,SCENARIO)#'shopify/Customers/backfill'
        PROCCESSED_PREFIX = 'shopify/customers/ods/backfill'
    if SCENARIO == 'backfill_daily':
        BATCH_PREFIX = '{}/{}/{}'.format(SOURCE,API,SCENARIO)#'shopify/Customers/backfill_daily'
        PROCCESSED_PREFIX = 'shopify/customers/ods/backfill'
    if SCENARIO == 'reconcile_daily':
        BATCH_PREFIX = '{}/{}/{}'.format(SOURCE,API,SCENARIO)#'shopify/Customers/reconcile_daily'
        PROCCESSED_PREFIX = 'shopify/customers/ods/reconcile'
    #PROCCESSED_PREFIX = '{}/{}/{}/{}'.format(SOURCE,API,PROCESS_TARGET,LOAD_TYPE)   #'shopify/Customers/ods/incremental'

    #error log file definition:
    LOG_FILE_PATH = config['log_file']['log_file_path']
    LOG_FILE_PREFIX = config['log_file']['log_file_prefix_customers_incremental']
    LOG_FILE_SUFFIX = config['log_file']['log_file_suffix']   #'.log'
    LOG_FILE_TIMESTAMP = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    LOG_FILE = '{}{}_{}{}'.format(LOG_FILE_PATH ,LOG_FILE_PREFIX ,LOG_FILE_TIMESTAMP,LOG_FILE_SUFFIX)
    # set up error file logger:
    fh = logging.FileHandler(filename=LOG_FILE, mode='a')
    fh.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    f_logger = logging.getLogger('f_logger')
    f_logger.addHandler(fh)

    #lod db definition:
    LOG_DB_HOST = config['log_db']['log_db_host']
    LOG_DB_USER = config['log_db']['log_db_user']
    LOG_DB_PWD = config['log_db']['log_db_pwd']
    LOG_DB = config['log_db']['log_db_name']
    LOG_TABLE = config['log_db']['log_table']
    #connection to log db:
    log_conn = psycopg2.connect(host=LOG_DB_HOST,dbname=LOG_DB, user=LOG_DB_USER, password=LOG_DB_PWD)
    log_cursor = log_conn.cursor()
    #define db loggers:
    logdb = LogDBHandler(log_conn, log_cursor, LOG_TABLE, DAG_ID, SCRIPT_NAME)
    db_logger = logging.getLogger('dblog')
    db_logger.setLevel(logging.DEBUG)
    db_logger.addHandler(logdb)

    #load json files into bq:
    db_logger.info('Shopify Customers - ODS Load started')
    #get batch list:
    batch_list = get_batch_list()
    #multiprocess:
    db_logger.info('Multiprocess started...')
    with ThreadPoolExecutor() as executor:
        executor.map(load_json_bq, batch_list)
    db_logger.info('Shopify Customers - ODS Load competed')
