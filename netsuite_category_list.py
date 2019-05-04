
import jaydebeapi
import os
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
import time
from google.cloud import storage
from google.cloud import bigquery
import configparser as cp
import logging
from LogDBHandler import LogDBHandler
from UpdateLogDBHandler import UpdateLogDBHandler
from UpdateRecordCountLogDBHandler import UpdateRecordCountLogDBHandler
from NetsuiteAuditInsertHandler import NetsuiteAuditInsertHandler
import set_environ_vars
import sys
import json
from string import whitespace
from airflow.models import Variable
from bq_maintanence import bq_table_update

def main():
    if len(sys.argv) == 4:
        SCENARIO = sys.argv[1]
        INCR = int(sys.argv[2])
        DAG_ID = sys.argv[3]
    else:
        sys.exit()

    #set up configs:
    SCRIPT_NAME =  os.path.basename(__file__)
    CONFIG_PATH = os.environ['CONFIG_PATH']
    config = cp.ConfigParser()
    config.read(CONFIG_PATH + 'netsuite.ini')
    #load configs:
    SOURCE='netsuite'
    TABLE_NAME = 'category_list'
    PROCESS_TARGET = 'ods'
    BATCH_DT = datetime.utcnow()

    #error log file definition:
    LOG_FILE_PATH = config['log_file']['log_file_path']
    LOG_FILE_SUFFIX = config['log_file']['log_file_suffix']
    LOG_FILE_TIMESTAMP = BATCH_DT.strftime('%Y%m%d%H%M%S')
    LOG_FILE = '{}{}_{}_{}{}'.format(LOG_FILE_PATH ,TABLE_NAME, SCENARIO,LOG_FILE_TIMESTAMP,LOG_FILE_SUFFIX)
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
    LOG_AUDIT_TABLE = config['log_db']['log_audit_table']
    #connection to log db:
    log_conn = psycopg2.connect(host=LOG_DB_HOST,dbname=LOG_DB, user=LOG_DB_USER, password=LOG_DB_PWD)
    log_cursor = log_conn.cursor()
    #define db loggers:
    logdb = LogDBHandler(log_conn, log_cursor, LOG_TABLE, DAG_ID, SCRIPT_NAME)
    db_logger = logging.getLogger('dblog')
    db_logger.setLevel(logging.DEBUG)
    db_logger.addHandler(logdb)

    #to insert new audit record:
    sql1 = 'insert into public.{}(batch_start_dt,batch_end_dt,scenario,source_table, target_table) values (!, ?, [], $, @) returning rid::TEXT;'.format(LOG_AUDIT_TABLE)
    insert_audit_handler = NetsuiteAuditInsertHandler(log_conn, log_cursor, LOG_TABLE, DAG_ID, SCRIPT_NAME, sql1,'AUDIT_RID')
    insert_audit_logger = logging.getLogger('insert_audit_logdb')
    insert_audit_logger.setLevel(logging.DEBUG)
    insert_audit_logger.addHandler(insert_audit_handler)
    #to update config table with file count:
    sql2 = 'update public.{} set record_count=[]::int where rid = ?::int; select 1::TEXT as status;'.format(LOG_AUDIT_TABLE)
    update_audit_rc_handler = UpdateRecordCountLogDBHandler(log_conn, log_cursor, LOG_TABLE, DAG_ID, SCRIPT_NAME, sql2,'UPDATE_STATUS')
    update_audit_rc_logger = logging.getLogger('update_audit_rc_logdb')
    update_audit_rc_logger.setLevel(logging.DEBUG)
    update_audit_rc_logger.addHandler(update_audit_rc_handler)
    #to update config table with success:
    sql3 = 'update public.{} set is_success = 1::bit where rid = ?::int; select 1::TEXT as status;'.format(LOG_AUDIT_TABLE)
    update_audit_success_handler = UpdateLogDBHandler(log_conn, log_cursor, LOG_TABLE, DAG_ID, SCRIPT_NAME, sql3,'UPDATE_STATUS')
    update_audit_success_logger = logging.getLogger('update_audit_success_logdb')
    update_audit_success_logger.setLevel(logging.DEBUG)
    update_audit_success_logger.addHandler(update_audit_success_handler)

    #gcp configs:
    SERVICE_ACCOUNT_KEY_FILE = config['gcp']['service_account_key_file']
    BUCKET_NAME = config['gcp']['bucket_name']
    CLIENT = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_FILE)
    BUCKET = CLIENT.get_bucket(BUCKET_NAME)

    #BigQuery configs:
    DATASET_ID = config['gcp']['dataset_id']
    PROJECT_ID = config['gcp']['project_id']

    #Netsuite connection:
    NS_CONN_STR = config['netsuite']['conn']
    NS_DRIVER_LOC = config['netsuite']['driver_loc']
    NS_USER = config['netsuite']['user']
    NS_PWD = config['netsuite']['pwd']
    NS_CREDENTIALS = {'user': NS_USER, 'password': NS_PWD}

    if SCENARIO == 'incremental_daily':
        #calculate previous day range: #'2018-08-07 01:28:20'
        current_date_time = datetime.utcnow()
        last_date_time = current_date_time - timedelta(days = INCR)
        #calculate date range for extract:
        START_DT =  last_date_time.strftime('%Y-%m-%d 00:00:00')
        END_DT = current_date_time.strftime('%Y-%m-%d 00:00:00')
    if SCENARIO == 'incremental_hourly':
        #calculate previous day range: #'2018-08-07 01:28:20'
        current_date_time = datetime.utcnow()
        last_date_time = current_date_time - timedelta(hours = INCR)
        START_DT =  last_date_time.strftime('%Y-%m-%d %H:00:00')
        END_DT = current_date_time.strftime('%Y-%m-%d %H:00:00')
    if SCENARIO == 'initial':
        current_date_time = datetime.utcnow()
        last_date_time = datetime(1900, 1, 1)
        #calculate date range for extract:
        START_DT =  last_date_time.strftime('%Y-%m-%d 00:00:00')
        END_DT = current_date_time.strftime('%Y-%m-%d 00:00:00')
    if SCENARIO == 'backfill_custom':
        START_DT =  datetime.strptime(Variable.get("netsuite_backfill_start_dt"), '%Y-%m-%d').strftime('%Y-%m-%d 00:00:00')
        END_DT = datetime.strptime(Variable.get("netsuite_backfill_end_dt"), '%Y-%m-%d').strftime('%Y-%m-%d 00:00:00')

    # tagret file :
    file_start = START_DT.replace(' ','').replace('-','').replace(':','')[0:14]
    file_timestamp = BATCH_DT.strftime('%Y%m%d%H%M%S')
    file_ext = '.csv'
    filename = '{}_{}_{}{}'.format(TABLE_NAME,file_start,file_timestamp,file_ext)  #file name pattern
    d = '{}/{}/{}/{}'.format(SOURCE,TABLE_NAME,SCENARIO,filename)

    #extract prep:
    TIMESTAMP_COLUMN = 'LAST_MODIFIED_DATE'
    COLUMN_STR = '''CATEGORY_LIST_EXTID
    ,CATEGORY_LIST_ID
    ,CATEGORY_LIST_NAME
    ,DATE_CREATED
    ,IS_INACTIVE
    ,LAST_MODIFIED_DATE
    ,PARENT_ID
    '''
    sql= 'select {} from {} where {} between \'{}\' and \'{}\';'.format(COLUMN_STR, TABLE_NAME, TIMESTAMP_COLUMN,  START_DT, END_DT)
    #get rid of whitespaces and split:
    COLUMN_LIST = COLUMN_STR.translate(dict.fromkeys(map(ord, whitespace))).split(',')

    #########process start:
    #log the start:
    db_logger.info('Netsuite {} {} extract start. All configs are set'.format(TABLE_NAME, SCENARIO))
    #extract data:
    #insert start and end date , and scenario into audit table. bring back newly generated rid:
    insert_audit_logger.info('Inserting new audit record. Batch start-end-scenario-source-target: {} {} {} {} {}'.format(START_DT.replace(' ','T'),END_DT.replace(' ','T'),SCENARIO,TABLE_NAME,TABLE_NAME))
    rid = os.environ['AUDIT_RID']
    db_logger.info('{} audit rid: {}'.format(TABLE_NAME,rid))
    #retry loop in case of unsuccessful call five times:
    success=0
    try_count=1
    while success==0 and try_count<=5:
        try:
            conn = jaydebeapi.connect("com.netsuite.jdbc.openaccess.OpenAccessDriver",
                                   NS_CONN_STR,
                                  NS_CREDENTIALS,
                                   NS_DRIVER_LOC
                                   )
            cur = conn.cursor()
            cur.execute(sql)
            df = pd.DataFrame(cur.fetchall(), columns = COLUMN_LIST)
            cur.close()
            conn.close()
            rec_count = df.shape[0]
            update_audit_rc_logger.info('Updating audit record count to {} for RID {}'.format(rec_count,rid))
            update_status = os.environ['UPDATE_STATUS']
            db_logger.info('Audit update status: %s' % update_status)
            success = 1
        except Exception as e:
            if try_count==5:  #on last try iteration exit function with empty list
                db_logger.error('Netsuite {} extract Failed!. See details in the log file {}'.format(TABLE_NAME, LOG_FILE))
                f_logger.error('Netsuite {} extract Failed!'.format(TABLE_NAME), exc_info=True, stack_info=True)
                sys.exit()
            time.sleep(5)
            try_count +=1

    if not df.empty:
        #1. upload data into storage as CSV:
        serializeddata = df.to_csv(sep=',', header=True, index = False)
        #retry loop in case of unsuccessful call five times:
        success=0
        try_count=1
        while success==0 and try_count<=5:
            try:
                #The blob method create the new file, also an object.
                d = BUCKET.blob(d)
                d.upload_from_string(serializeddata)
                db_logger.info('{} file upload succcess: {}'.format(TABLE_NAME,d.name))
                success = 1
            except Exception as e:
                if try_count==5:  #on last try iteration exit function with empty list
                    db_logger.error('Netsuite {} extract Failed!. See details in the log file {}'.format(TABLE_NAME, LOG_FILE))
                    f_logger.error('Netsuite {} extract Failed!'.format(TABLE_NAME), exc_info=True, stack_info=True)
                    sys.exit()
                time.sleep(1)
                try_count +=1

        #2. load data to BQ;
        # data type conversion:
        df['CATEGORY_LIST_EXTID'] = df['CATEGORY_LIST_EXTID'].astype('O')
        df['CATEGORY_LIST_ID'] = df['CATEGORY_LIST_ID'].fillna(0).astype('int64')
        df['CATEGORY_LIST_NAME'] = df['CATEGORY_LIST_NAME'].astype('O')
        df['DATE_CREATED'] = pd.to_datetime(df['DATE_CREATED'])
        df['IS_INACTIVE'] = df['IS_INACTIVE'].astype('O')
        df['LAST_MODIFIED_DATE'] = pd.to_datetime(df['LAST_MODIFIED_DATE'])
        df['PARENT_ID'] = df['PARENT_ID'].fillna(0).astype('int64')
        df['ods_inserted_at'] = BATCH_DT

        #retry loop in case of unsuccessful call five times:
        success=0
        try_count=1
        while success==0 and try_count<=5:
            try:
                df.to_gbq('{}.{}'.format(DATASET_ID, TABLE_NAME),
                        PROJECT_ID,
                        chunksize=None,
                        if_exists='append',
                        private_key=SERVICE_ACCOUNT_KEY_FILE
                        )
                db_logger.info('{} table load suscess.'.format(TABLE_NAME))
                success = 1
            except Exception as e:
                if try_count==5:  #on last try iteration exit function with empty list
                    db_logger.error('Netsuite {} extract Failed!. See details in the log file {}'.format(TABLE_NAME, LOG_FILE))
                    f_logger.error('Netsuite {} extract Failed!'.format(TABLE_NAME), exc_info=True, stack_info=True)
                    sys.exit()
                time.sleep(5)
                try_count +=1

        #3. update bq current table:
        BQ_CLIENT = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_FILE)
        TARGET_DATASET ='netsuite_current'
        LOCATION ='US'
        TARGET_TABLE = TABLE_NAME
        WRITE_OPTION = 'WRITE_TRUNCATE'  #overwrite on each load
        QUERY = '''
            SELECT * EXCEPT(row_number, ods_inserted_at)
            FROM (SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY category_list_ID order by last_modified_date desc) row_number
                FROM {}.{})
            WHERE row_number = 1'''.format(DATASET_ID,TABLE_NAME)

        db_logger.info('{}.{} table update starts now...'.format(TARGET_DATASET,TARGET_TABLE))
        try:
            query_row_count,target_table, target_row_count = bq_table_update(BQ_CLIENT,LOCATION,TARGET_DATASET,TARGET_TABLE,WRITE_OPTION,QUERY)
            db_logger.info('{} table update suscess: query rc - {}, table rc - {}'.format(target_table, query_row_count,target_row_count))
        except Exception as e:
            db_logger.error('{}.{} update Failed!. See details in the log file {}'.format(TARGET_DATASET,TARGET_TABLE, LOG_FILE))
            f_logger.error('{}.{} update Failed!'.format(TARGET_DATASET,TARGET_TABLE), exc_info=True, stack_info=True)

    update_audit_success_logger.info('Updating audit to success. RID: %s' % rid)
    update_status = os.environ['UPDATE_STATUS']
    db_logger.info('Audit update status: %s' % update_status)
    db_logger.info('Netsuite {} extract complete.'.format(TABLE_NAME))

if __name__ == '__main__':
    main()
