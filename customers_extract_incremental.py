import requests
from datetime import datetime, timedelta
import time
import json
import psycopg2
import configparser as cp
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from LogDBHandler import LogDBHandler
#from GetLogDBHandler import GetLogDBHandler
from UpdateLogDBHandler import UpdateLogDBHandler
from UpdateFileCountLogDBHandler import UpdateFileCountLogDBHandler
from InsertAuditLogDBHandler import InsertAuditLogDBHandler
import set_environ_vars
import sys

'''
INCREMENTAL EXTRACT - based on updated_at values
Scenario 1: (hourly)
- calculate start and end dts for the previous hour
- make multiple retries to fetch data out
- log and audit updates
- exit if fails

Scenario 2: (backfill)  - manual or once a day
- check if any of the last 23 batches failed or missed (get info from audit table)
- retry to extract the data
- log and audit updates
- exit if fails

Scenario 3: (daily_backfill)  - manual or once a day - by updated_at
- calculate start and end dts for the previous day
- make multiple retries to fetch data out
- log and audit updates
- exit if fails

Scenario 4: (reconcile) - once a day - by created_at
- calculate start and end dts for the previous day
- make multiple retries to fetch data out
- log and audit updates
- exit if fails
'''


def get_page_list():
    '''compose a page url list to iterate through:
    - get Customers count
    - calc page number as Customers count divided by page limited
    - compose list of page urls by iterating through range(1, page count +1)'''

    #get Customers count: /admin/Customers/count.json
    count_url = 'https://{}:{}@fnova.myshopify.com/admin/{}/count.json?&{}'.format(API_USER,API_PWD,API,REQUEST_CONDITION)
    db_logger.info('Customers count URL: %s' % count_url)
    #retry loop in case of unsuccessful call five times:
    success=0
    try_count=1
    while success==0 and try_count<=5:
        try:
            r = requests.get(count_url)
            status_code = r.status_code
            if status_code != 200: #bubble up error after five attempts
                raise RuntimeError('Customers Count Request failed! Status Code: {}. Page URL: {}'.format(status_code, page_url[0]))
            success = 1
        except Exception as e:
            if try_count==5:  #on last try iteration exit function with empty list
                db_logger.error('Customers Count Request Failed!. See details in the log file %s' % LOG_FILE)
                f_logger.error('Customers Count Request Failed!', exc_info=True, stack_info=True)
                return ([],'0')
            time.sleep(1+try_count)
            try_count +=1

    item_count = r.json()['count']
    page_count = 0
    if item_count % PAGE_LIMIT > 0:
        page_count = item_count // PAGE_LIMIT + 1
    else:
        page_count = item_count // PAGE_LIMIT
    #logging:
    db_logger_rec_count.info('%s' % item_count)
    db_logger_page_count.info('%s' % page_count)
    #define nested helper function:
    def page_url(i):
        return 'https://{}:{}@fnova.myshopify.com/admin/{}.json?status=any&{}&limit={}&page={}'.format(API_USER,API_PWD,API,REQUEST_CONDITION, PAGE_LIMIT, i)
    #return list:
    return ([(page_url(i),i) for i in range(1,page_count+1)],item_count)


#this function to be mapped to items from page url list for multiprocessing:
def load_api_json(page_url):
    '''make a request to pull page data and load it as json file to GCP storage bucket.
    No logging to db - concurrency issues - processes are blocking one another'''

    #Now define the path within your bucket and the file name:
    file_start = START_DT.replace('T','').replace('-','').replace(':','')[0:14]
    file_timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S') #UTC time!!!
    file_ext = '.json'
    filename = '{}_{}_{}_{}{}'.format(API,file_start,file_timestamp,page_url[1],file_ext)  #file name pattern
    d = '{}/{}/{}/{}'.format(SOURCE,API,LOAD_TYPE,filename) #shopify/Customers/initial/orders_20170101000000_20181205122343_1.json'
    #print(page_url) #comment it out!!! no db logging :(
    #retry loop in case of unsuccessful call five times:
    success=0
    try_count=1
    while success==0 and try_count<=5:
        try:
            r = requests.get(page_url[0])
            status_code = r.status_code
            if status_code != 200:
                raise RuntimeError('Page Request failed! Status Code: {}. Page URL: {}'.format(status_code, page_url[0]))
            success = 1
        except Exception as e:
            if try_count ==5:
                f_logger.error('Page request failed!', exc_info=True, stack_info=True)
                return
            time.sleep(1+try_count)
            try_count +=1
    #get json dataset:
    page_items = r.json()[API]
    serializeddata = json.dumps(page_items)
    #The blob method create the new file, also an object.
    d = BUCKET.blob(d)
    #simple 5-time retry:
    success=0
    try_count=1
    while success==0 and try_count<=5:
        try:
            d.upload_from_string(serializeddata)
            success = 1
        except Exception as e:
            if try_count==5:
                f_logger.error('JSON upload failed!', exc_info=True, stack_info=True)
                return
            time.sleep(1+try_count)
            try_count +=1

def date_to_stamp(date_str):
    return date_str.replace('T','').replace('-','').replace(':','')[0:14]

def is_batch_complete():
    '''check if all the files materialized in the bucket. 1 - yes, 0 - no'''

    #pull the blob iterator:
    blob_list = BUCKET.list_blobs(prefix=BATCH_PREFIX)
    file_start_stamp = date_to_stamp(START_DT)
    global batch_blob_list
    batch_blob_list = [bl for bl in blob_list if bl.name.split('_')[-3] == file_start_stamp]
    batch_file_count = len(batch_blob_list)
    db_logger.info('Customers actual file count: %s' % batch_file_count)
    if batch_file_count != page_num:
        db_logger.info('Customers file count doesnt match page count')
        return 0
    else:
        return 1

#######################   execution starts here:   ##########################
#SCENARIO = ''
#DAG_ID = ''
def main():
    global SCENARIO
    global DAG_ID
    if len(sys.argv) == 3:
        SCENARIO = sys.argv[1]
        DAG_ID = sys.argv[2]
    else:
        sys.exit()

if __name__ == '__main__':
    # sys.exit(main(sys.argv)) # used to give a better look to exists
    main()
    #load configs:
    LOAD_TYPE = 'incremental'
    #DAG_ID = 'shopify_orders_incremental_{}'.format(SCENARIO)
    SCRIPT_NAME =  os.path.basename(__file__)
    CONFIG_PATH = os.environ['CONFIG_PATH']
    config = cp.ConfigParser()
    config.read(CONFIG_PATH + 'shopify.ini')

    #source configs:
    SOURCE = config['api']['api_source']   #'shopify'
    API = config['api']['api_customers']   #'Customers'
    API_USER = config['api']['api_user']   #'ae45b158125c41eed1efd3183e752e1c'
    API_PWD = config['api']['api_pwd']   #'557f2a26c8dece721f4930d2881de069'
    PAGE_LIMIT = int(config['api']['api_page_limit'])   #250
    #target configs:
    SERVICE_ACCOUNT_KEY_FILE = config['gcp']['service_account_key_file']
    BUCKET_NAME = config['gcp']['bucket_name']
    CLIENT = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_FILE)
    BUCKET = CLIENT.get_bucket(BUCKET_NAME)
    #BATCH_PREFIX = 'shopify/Customers/incremental' #'shopify/Customers/incomplete_batches/initial'#
    if SCENARIO == 'hourly':
        BATCH_PREFIX = 'shopify/customers/incremental'
    #AUDIT table containing date ranges will drive the extract execution in backfill scenario.
#GetLogDBHandler is responsible for bringing config info in.
#Insert/UpdateLogDBHandlers  - for creatin new audit records and updates.
#logging is limited to error logs into files. DBLogger is used from only routines that are not running in multiprocessing

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
    LOG_AUDIT = config['log_db']['log_audit_customers_incremental']
    #connection to log db:
    log_conn = psycopg2.connect(host=LOG_DB_HOST,dbname=LOG_DB, user=LOG_DB_USER, password=LOG_DB_PWD)
    log_cursor = log_conn.cursor()
    #define db loggers:
    logdb = LogDBHandler(log_conn, log_cursor, LOG_TABLE, DAG_ID, SCRIPT_NAME)
    db_logger = logging.getLogger('dblog')
    db_logger.setLevel(logging.DEBUG)
    db_logger.addHandler(logdb)
    #define db logger for record count
    db_logger_rec_count = logging.getLogger('dblog_rec_count')
    db_logger_rec_count.setLevel(logging.DEBUG)
    db_logger_rec_count.addHandler(logdb)
    #define db logger for page count
    db_logger_page_count = logging.getLogger('dblog_page_count')
    db_logger_page_count.setLevel(logging.DEBUG)
    db_logger_page_count.addHandler(logdb)

    #to update config table with success:
    sql3 = 'update public.{} set is_success = 1::bit where rid = ?::int; select 1::TEXT as status;'.format(LOG_AUDIT)
    update_iter_conf_logdb = UpdateLogDBHandler(log_conn, log_cursor, LOG_TABLE, DAG_ID, SCRIPT_NAME, sql3,'UPDATE_STATUS')
    update_db_logger = logging.getLogger('update_conf_logdb')
    update_db_logger.setLevel(logging.DEBUG)
    update_db_logger.addHandler(update_iter_conf_logdb)

    #to update config table with file count:
    sql4 = 'update public.{} set file_count=!::int, record_count=[]::int where rid = ?::int; select 1::TEXT as status;'.format(LOG_AUDIT)
    update_conf_file_count_handler = UpdateFileCountLogDBHandler(log_conn, log_cursor, LOG_TABLE, DAG_ID, SCRIPT_NAME, sql4,'UPDATE_STATUS')
    update_conf_file_count_logger = logging.getLogger('update_conf_filecount_logdb')
    update_conf_file_count_logger.setLevel(logging.DEBUG)
    update_conf_file_count_logger.addHandler(update_conf_file_count_handler)

    #to insert new audit record:
    sql5 = 'insert into public.{}(batch_start_dt,batch_end_dt,scenario) values (!, ?, []) returning rid::TEXT;'.format(LOG_AUDIT,)
    insert_audit_handler = InsertAuditLogDBHandler(log_conn, log_cursor, LOG_TABLE, DAG_ID, SCRIPT_NAME, sql5,'AUDIT_RID')
    insert_audit_logger = logging.getLogger('insert_audit_logdb')
    insert_audit_logger.setLevel(logging.DEBUG)
    insert_audit_logger.addHandler(insert_audit_handler)

    #print('Customers extract start. All configurations are set.')   #comment it out!!!
    db_logger.info('Customers extract incremental start. All configurations are set.')

    batch_blob_list =[]

    if SCENARIO == 'hourly':
        #calculate previous hour range:
        current_hour_date_time = datetime.utcnow()
        last_hour_date_time = datetime.utcnow() - timedelta(hours = 1)
        START_DT =  last_hour_date_time.strftime('%Y-%m-%dT%H:00:00-00:00')
        END_DT = current_hour_date_time.strftime('%Y-%m-%dT%H:00:00-00:00')
        REQUEST_CONDITION    = 'updated_at_min={}&updated_at_max={}'.format(START_DT,END_DT)
        #insert start and end date , and scenario into audit table. bring back newly generated rid:
        insert_audit_logger.info('Inserting new audit record. Batch start-end-scenario: {} {} {}'.format(START_DT,END_DT,SCENARIO))
        rid = os.environ['AUDIT_RID']
        db_logger.info('Customers audit rid: %s' % rid)
        #get page list:
        page_list,rec_count = get_page_list()
        page_num = len(page_list)
        #update audit table with file/record count:!!!
        update_conf_file_count_logger.info('Updating audit file and record count to {} and {} for RID {}'.format(page_num,rec_count,rid))
        update_status = os.environ['UPDATE_STATUS']
        db_logger.info('Customers audit update status: %s' % update_status)
        if page_num == 0:
            db_logger.info('Page list is empty.')
        else:
            db_logger.info('Page list created. Starting multiprocessing...')
        #multiprocess:
            with ThreadPoolExecutor() as executor:
                executor.map(load_api_json, page_list)

        '''we expect data to change on the go so just push through what we can
        #check if batch complete:
        batch_check = is_batch_complete()
        if batch_check == 1:
            #update config table with success:
            update_db_logger.info('Updating audit to success. RID: %s' % rid)
            update_status = os.environ['UPDATE_STATUS']
            db_logger.info('Customers audit update status: %s' % update_status)
            db_logger.info('Customers incremental extract success.')
        if batch_check == 0:
            #delete batch files:
            for blob in batch_blob_list:
                blob.delete()
            db_logger.critical('Customers incremental extract failed. Batch files are being deleted...')
            '''
        db_logger.info('Customers incremental extract complete.')
