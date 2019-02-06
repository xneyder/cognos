#!/bin/python
#Daniel Jaramillo March 2018
#Cognos
import subprocess
import datetime
import os
import glob
import time
import base64
import logging
import sys
import cx_Oracle
from Queue import Queue
from threading import Thread
from dateutil.relativedelta import relativedelta
from logging.handlers import TimedRotatingFileHandler

INPUT_KPI_NAME=''
if len(sys.argv)>1:
    INPUT_KPI_NAME="AND KPI_NAME='{KPI_NAME}'".format(KPI_NAME=sys.argv[1])

FORMATTER=logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
DB_USER=os.environ['DB_USER']
DB_PASSWORD=base64.b64decode(os.environ['DB_PASSWORD'])
ORACLE_SID=os.environ['ORACLE_SID']
DB_HOST=os.environ['DB_HOST']
LOAD_METADATA_SLEEP_INTERVAL=300
BCP_IN_DIR=os.environ['RAW_DATA_DIR']+'/INPUTS/COGNOS/'
if not os.path.exists(BCP_IN_DIR):
    os.makedirs(BCP_IN_DIR)    
BCP_DONE_DIR=os.environ['RAW_DATA_DIR']+'/COMPLETED/COGNOS/'
if not os.path.exists(BCP_DONE_DIR):
    os.makedirs(BCP_DONE_DIR)

LOG_DIR=os.environ['LOG_DIR']+'/COGNOS/'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE=LOG_DIR+'/cognos_cx_oracle.log'
TMP_DIR=LOG_DIR
if not os.path.exists(TMP_DIR):
    os.makedirs(TMP_DIR)
RESOLUTIONS=[
    {'name':'5MIN','offset':10,'column_formula':'AGGR_5M','column_source_resolution':'AGGR_TABLE_EXT_5M','max_history_mins':1440},
    {'name':'15MIN','offset':20,'column_formula':'AGGR_15M','column_source_resolution':'AGGR_TABLE_EXT_15M','max_history_mins':1440},
    {'name':'HH','offset':80,'column_formula':'AGGR_HH','column_source_resolution':'AGGR_TABLE_EXT_HH','max_history_mins':1440},
    {'name':'DD','offset':360,'column_formula':'AGGR_DY','column_source_resolution':'AGGR_TABLE_EXT_DY','max_history_mins':10080},
    {'name':'IW','offset':600,'column_formula':'AGGR_IW','column_source_resolution':'AGGR_TABLE_EXT_IW','max_history_mins':43200},
    {'name':'MM','offset':780,'column_formula':'AGGR_MO','column_source_resolution':'AGGR_TABLE_EXT_MO','max_history_mins':259200},
    ]
processes_queue=Queue()
threads = 30
sma_details=[]
kpi_details=[]
device_details=[]

class ManagedDbConnection:
    def __init__(self, DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST):
        self.DB_USER = DB_USER
        self.DB_PASSWORD = DB_PASSWORD
        self.ORACLE_SID = ORACLE_SID
        self.DB_HOST = DB_HOST

    def __enter__(self):
        try:
            self.db = cx_Oracle.connect('{DB_USER}/{DB_PASSWORD}@{DB_HOST}/{ORACLE_SID}'.format(DB_USER=self.DB_USER,DB_PASSWORD=self.DB_PASSWORD,DB_HOST=self.DB_HOST,ORACLE_SID=self.ORACLE_SID), threaded=True)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(e)
            quit()
        self.cursor = self.db.cursor()
        sqlplus_script="alter session set nls_date_format = 'DD-MON-YY HH24:MI'"
        try:
            self.cursor.execute(sqlplus_script)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(e)
            app_logger.error(sqlplus_script[0:900])
            quit()
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()


def get_console_handler():
    console_handler=logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler

def get_file_handler():
    file_handler=TimedRotatingFileHandler(LOG_FILE,when='midnight')
    file_handler.setFormatter(FORMATTER)
    return file_handler

def get_logger(logger_name):
    logger=logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    #logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    logger.propagate=False
    return logger

def run_sqlldr(ctl_file,log_file,DB_USER,DB_PASSWORD,ORACLE_SID):

    """
    Run sqlldr for the given control file.
    """  
    p = subprocess.Popen(['sqlldr',
        '{DB_USER}/{DB_PASSWORD}@{ORACLE_SID}'.format(DB_USER=DB_USER,DB_PASSWORD=DB_PASSWORD,ORACLE_SID=ORACLE_SID),
        'control={ctl_file}'.format(ctl_file=ctl_file),
        'log={log_file}'.format(log_file=log_file)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    (stdout,stderr) = p.communicate()
    stdout_lines = stdout.decode('utf-8').split("\n")
    return p.returncode,stdout_lines

def build_ar_dict(keys,cursor):
    """
    Zip keys and values to a dictionary and build a list of them
    """
    data_dictionary=[]
    for row in filter(None,cursor):
        data_dictionary.append(dict(zip(keys, row)))
    return data_dictionary

def load_metada():
    with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
        cursor=db.cursor()
        global sma_details
        global kpi_details
        global device_details
        app_logger.info('Loading Metadata')
        sqlplus_script="""
        SELECT A.SMA_NAME,
        A.DEVICE_GROUP_NAME,
        A.ADDITIONAL_CRITERIA,
        A.DESTINATION_TABLE,
        A.DESTINATION_DB,
        A.DESTINATION_DB_HOST,
        A.DESTINATION_USER_ID,
        A.DESTINATION_PW 
        FROM PMMCONF_DB.SMA_DETAILS A
        WHERE A.ACTIVE=1
        """
        try:
            cursor.execute(sqlplus_script)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(str(e)+" --- "+sqlplus_script.replace('\n',' '))
            return False

        keys=['SMA_NAME','DEVICE_GROUP_NAME','ADDITIONAL_CRITERIA','DESTINATION_TABLE','DESTINATION_DB','DESTINATION_DB_HOST', 'DESTINATION_USER_ID','DESTINATION_PW' ]
        sma_details= build_ar_dict(keys,cursor)

        sqlplus_script="""
        SELECT 
        A.SMA_NAME,
        A.KPI_NAME,
        A.UNITS,
        A.SOURCE_BASE_TABLE,
        A.KPI_EXPRESSION,
        A.ADDITIONAL_CRITERIA,
        A.AGGR_TO_DEVICE,
        A.AGGR_5M,
        A.AGGR_15M,
        A.AGGR_HH,
        A.AGGR_DY,
        A.AGGR_IW,
        A.AGGR_MO,
        A.AGGR_TABLE_EXT_5M,
        A.AGGR_TABLE_EXT_15M,
        A.AGGR_TABLE_EXT_HH,
        A.AGGR_TABLE_EXT_DY,
        A.AGGR_TABLE_EXT_IW,
        A.AGGR_TABLE_EXT_MO,
        A.DEVICE_FIELD_NAME,
        A.DEVICE_TARGET_FIELD
        FROM PMMCONF_DB.SMA_KPI_DETAILS A, PMMCONF_DB.SMA_DETAILS B
        WHERE A.ACTIVE=1
        and B.ACTIVE=1
        and A.SMA_NAME=B.SMA_NAME
        {INPUT_KPI_NAME}
        """.format(INPUT_KPI_NAME=INPUT_KPI_NAME)
        # print (sqlplus_script)
        try:
            cursor.execute(sqlplus_script)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(str(e)+" --- "+sqlplus_script.replace('\n',' '))
            return False


        keys=['SMA_NAME','KPI_NAME','UNITS','SOURCE_BASE_TABLE','KPI_EXPRESSION','ADDITIONAL_CRITERIA','AGGR_TO_DEVICE','AGGR_5M','AGGR_15M','AGGR_HH','AGGR_DY','AGGR_IW','AGGR_MO','AGGR_TABLE_EXT_5M','AGGR_TABLE_EXT_15M','AGGR_TABLE_EXT_HH','AGGR_TABLE_EXT_DY','AGGR_TABLE_EXT_IW','AGGR_TABLE_EXT_MO','DEVICE_FIELD_NAME','DEVICE_TARGET_FIELD' ]
        kpi_details= build_ar_dict(keys,cursor)

        sqlplus_script="""
        SELECT 
        B.SMA_NAME,
        A.DEVICE_GROUP_NAME ,
        A.DEVICE,
        A.DEVICE_EXPRESSION
        FROM PMMCONF_DB.SMA_DEVICE_LIST A, PMMCONF_DB.SMA_DETAILS B
        WHERE B.ACTIVE=1
        and A.DEVICE_GROUP_NAME=B.DEVICE_GROUP_NAME
        """
        try:
            cursor.execute(sqlplus_script)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(str(e)+" --- "+sqlplus_script.replace('\n',' '))
            return False
        keys=['SMA_NAME','DEVICE_GROUP_NAME','DEVICE','DEVICE_EXPRESSION']
        device_details= build_ar_dict(keys,cursor)



def th_load_metada():
    while True:
        app_logger.info('Sleeping {LOAD_METADATA_SLEEP_INTERVAL} to load the metadata again'.format(LOAD_METADATA_SLEEP_INTERVAL=LOAD_METADATA_SLEEP_INTERVAL))
        time.sleep(LOAD_METADATA_SLEEP_INTERVAL)
        load_metada()



def enqueue(resolution,kpi_list=None):
    #get all the kpis for the resolution
    table_list=[]
    if kpi_list==None:
        kpi_list=[ x for x in kpi_details if x[resolution['column_formula']] ]

    for kpi in kpi_list:
        #Build the DATETIME round formula according to the resolution
        if resolution['name'] == '5MIN':
            DATETIME="trunc(DATETIME, 'hh24') +  round (to_char ( DATETIME,'MI' ) / 5 ) * 5 / 1440"
        elif resolution['name'] == '15MIN':
            DATETIME="trunc(DATETIME, 'hh24') +  round (to_char ( DATETIME,'MI' ) / 15 ) * 15 / 1440"
        elif resolution['name'] == 'HH':
            DATETIME="TRUNC(DATETIME,'HH')"
        elif resolution['name'] == 'DD':
            DATETIME="TRUNC(DATETIME)"
        elif resolution['name'] == 'IW':
            DATETIME="TRUNC(DATETIME, 'IW')"
        elif resolution['name'] == 'MM':
            DATETIME="TRUNC(DATETIME, 'MM')"

        #Get the sma details
        sma=filter(lambda x: x['SMA_NAME']==kpi['SMA_NAME'],sma_details)[0]
        if not sma:
            app_logger.error('SMA {sma} not found in sma_details table'.format(sma=kpi['SMA_NAME']))
            continue
        F_DEVICE_FIELD_NAME=kpi['DEVICE_FIELD_NAME'].split(',')[0]
        if kpi['DEVICE_FIELD_NAME'].startswith('FORMULA:'):
            F_DEVICE_FIELD_NAME=kpi['DEVICE_FIELD_NAME'].split(':')[1]
        if (sma['DEVICE_GROUP_NAME']):
            device_list=[x for x in device_details if x['DEVICE_GROUP_NAME']==sma['DEVICE_GROUP_NAME'] and x['DEVICE']]
            device_list=map(lambda x:"'"+x['DEVICE']+"'",device_list)
            device_list=','.join(device_list)
            device_criteria_list=[x for x in device_details if x['DEVICE_GROUP_NAME']==sma['DEVICE_GROUP_NAME'] and x['DEVICE_EXPRESSION']]
            device_criteria=""

            for index,item in enumerate(device_criteria_list):
                if index > 0:
                    device_criteria+="\nOR "
                else:
                    device_criteria+="("
                device_criteria+="{DEVICE_FIELD_NAME} like '{item}'".format(item=item['DEVICE_EXPRESSION'],
                    DEVICE_FIELD_NAME=F_DEVICE_FIELD_NAME)
            if device_criteria:
                device_criteria+=")"

        index=next((index for (index, x) in enumerate(table_list) if x["SOURCE_BASE_TABLE"] == kpi['SOURCE_BASE_TABLE'] and x["SMA_NAME"] == kpi['SMA_NAME'] and x["KPI_ADDITIONAL_CRITERIA"] == kpi['ADDITIONAL_CRITERIA']), None)
        if index >=0:
            table_list[index]['KPI_LIST'].append({
                'KPI_NAME':kpi['KPI_NAME'],
                'UNITS':kpi['UNITS'],
                'KPI_EXPRESSION':kpi['KPI_EXPRESSION'],
                'FORMULA':kpi[resolution['column_formula']],
                })
        else:            
            table_list.append({
                'SMA_NAME':kpi['SMA_NAME'],
                'SOURCE_BASE_TABLE':kpi['SOURCE_BASE_TABLE'],
                'DEVICE_FIELD_NAME':kpi['DEVICE_FIELD_NAME'],
                'F_DEVICE_FIELD_NAME':F_DEVICE_FIELD_NAME,
                'DEVICE_TARGET_FIELD':kpi['DEVICE_TARGET_FIELD'],
                'DATETIME':DATETIME,
                'AGGR_TYPE':resolution['name'],
                'SOURCE_RESOLUTION':kpi[resolution['column_source_resolution']],
                'DESTINATION_TABLE':sma['DESTINATION_TABLE'],
                'SMA_ADDITIONAL_CRITERIA':sma['ADDITIONAL_CRITERIA'],
                'DESTINATION_DB':sma['DESTINATION_DB'],
                'DESTINATION_DB_HOST':sma['DESTINATION_DB_HOST'],
                'DESTINATION_USER_ID':sma['DESTINATION_USER_ID'],
                'DESTINATION_PW':sma['DESTINATION_PW'],
                'DEVICE_LIST':device_list,
                'DEVICE_CRITERIA':device_criteria,
                'KPI_ADDITIONAL_CRITERIA':kpi['ADDITIONAL_CRITERIA'],
                'KPI_LIST':[{
                    'KPI_NAME':kpi['KPI_NAME'],
                    'UNITS':kpi['UNITS'],
                    'KPI_EXPRESSION':kpi['KPI_EXPRESSION'],
                    'FORMULA':kpi[resolution['column_formula']]
                    }]
                })

    for table in table_list:
        app_logger.info('Adding {sma} {table} {resolution} to queue'.format(sma=table['SMA_NAME'],table=table['SOURCE_BASE_TABLE'],resolution=table['AGGR_TYPE']))
        processes_queue.put(table)



def th_enqueue(resolution):
    """
    thread to enqueue kpi's processing according to the resolution
    """
    while True:
        enqueue(resolution)
        tm=datetime.datetime.now()
        if resolution['name'] == '5MIN':
            tm=tm-datetime.timedelta(minutes=tm.minute % 5,
                seconds=tm.second,
                microseconds=tm.microsecond)
            tm+=datetime.timedelta(minutes = 5)

        elif resolution['name'] == '15MIN':
            tm=tm-datetime.timedelta(minutes=tm.minute % 15,
                seconds=tm.second,
                microseconds=tm.microsecond)
            tm+=datetime.timedelta(minutes = 15)

        elif resolution['name'] == 'HH':
            tm=tm.replace(microsecond=0,second=0,minute=0)
            tm+=datetime.timedelta(minutes = 60)

        elif resolution['name'] == 'DD':
            tm=tm.replace(microsecond=0,second=0,minute=0,hour=0)
            tm+=datetime.timedelta(days = 1)

        elif resolution['name'] == 'IW':
            tm=tm-datetime.timedelta(days=tm.weekday(),
                hours=tm.hour,
                minutes=tm.minute,
                seconds=tm.second,
                microseconds=tm.microsecond)
            tm+=datetime.timedelta(days = 7)
        elif resolution['name'] == 'MM':
            tm=tm.replace(microsecond=0,second=0,minute=0,hour=0,day=1)            
            tm+=relativedelta(months=+1)

        sleep_until=tm+datetime.timedelta(minutes = resolution['offset']+1)
        app_logger.info('{resolution} sleeping until {sleep_until}'.format(sleep_until=sleep_until,resolution=resolution['name']))
        time.sleep(((sleep_until-datetime.datetime.now()).total_seconds()))

def th_run_now_kpi():
    with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
        cursor=db.cursor()
        while True:
            sqlplus_script="""        
            SELECT 
            A.SMA_NAME,
            A.KPI_NAME,
            A.UNITS,
            A.SOURCE_BASE_TABLE,
            A.KPI_EXPRESSION,
            A.ADDITIONAL_CRITERIA,
            A.AGGR_TO_DEVICE,
            A.AGGR_5M,
            A.AGGR_15M,
            A.AGGR_HH,
            A.AGGR_DY,
            A.AGGR_IW,
            A.AGGR_MO,
            A.AGGR_TABLE_EXT_5M,
            A.AGGR_TABLE_EXT_15M,
            A.AGGR_TABLE_EXT_HH,
            A.AGGR_TABLE_EXT_DY,
            A.AGGR_TABLE_EXT_IW,
            A.AGGR_TABLE_EXT_MO,
            A.DEVICE_FIELD_NAME,
            A.DEVICE_TARGET_FIELD
            FROM PMMCONF_DB.SMA_KPI_DETAILS A, PMMCONF_DB.SMA_DETAILS B
            WHERE A.ACTIVE=1
            and B.ACTIVE=1
            and A.SMA_NAME=B.SMA_NAME
            and RUN_NOW=1
            """
            try:
                cursor.execute(sqlplus_script)
            except cx_Oracle.DatabaseError as e:
                app_logger.error(str(e)+" --- "+sqlplus_script.replace('\n',' '))
                return False
            keys=['SMA_NAME','KPI_NAME','UNITS','SOURCE_BASE_TABLE','KPI_EXPRESSION','ADDITIONAL_CRITERIA','AGGR_TO_DEVICE','AGGR_5M','AGGR_15M','AGGR_HH','AGGR_DY','AGGR_IW','AGGR_MO','AGGR_TABLE_EXT_5M','AGGR_TABLE_EXT_15M','AGGR_TABLE_EXT_HH','AGGR_TABLE_EXT_DY','AGGR_TABLE_EXT_IW','AGGR_TABLE_EXT_MO','DEVICE_FIELD_NAME','DEVICE_TARGET_FIELD' ]
            run_now_kpi_details= build_ar_dict(keys,cursor)
            if run_now_kpi_details:
                load_metada()
            for kpi in run_now_kpi_details:
                for resolution in RESOLUTIONS:
                    if kpi[resolution['column_formula']] != '':
                        enqueue(resolution,[kpi])
                #Set run now equal to 0
                sqlplus_script="""            
                UPDATE PMMCONF_DB.SMA_KPI_DETAILS
                SET RUN_NOW=0
                WHERE SMA_NAME='{SMA_NAME}' AND KPI_NAME='{KPI_NAME}'
                """.format(
                SMA_NAME=kpi['SMA_NAME'],
                KPI_NAME=kpi['KPI_NAME']
                )
                try:
                    cursor.execute(sqlplus_script)
                except cx_Oracle.DatabaseError as e:
                    app_logger.error(str(e)+" --- "+sqlplus_script.replace('\n',' '))
                    return False
                db.commit()
            time.sleep(30)

def get_last_handled_datestamp(table):
    with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
        cursor=db.cursor()
        sqlplus_script="""    
        SELECT  MIN(TO_CHAR(LAST_HANDLED_DATESTAMP,'DD-MON-YY HH24:MI'))
        FROM PMMCONF_DB.SMA_KPI_DATE_CONTROL
        WHERE SMA_NAME='{SMA_NAME}' AND SOURCE_BASE_TABLE='{SOURCE_BASE_TABLE}' AND RESOLUTION='{RESOLUTION}'
        """.format(
            SMA_NAME=table['SMA_NAME'],SOURCE_BASE_TABLE=table['SOURCE_BASE_TABLE'],RESOLUTION=table['AGGR_TYPE'])
        try:
            cursor.execute(sqlplus_script)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(table['SMA_NAME']+" --- "+str(e)+" --- "+sqlplus_script.replace('\n',' '))
            return False
        # print(''.join(cursor.fetchall()[0]))
        return ''.join(filter(None,cursor.fetchall()[0]))

def update_last_handled_datestamp(table,update=True):
    with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
        cursor=db.cursor()
        if update==True:
            sqlplus_script="""        
            UPDATE PMMCONF_DB.SMA_KPI_DATE_CONTROL
            SET LAST_HANDLED_DATESTAMP=sysdate
            WHERE SMA_NAME='{SMA_NAME}' AND SOURCE_BASE_TABLE='{SOURCE_BASE_TABLE}' AND RESOLUTION='{RESOLUTION}'
            """.format(
                SMA_NAME=table['SMA_NAME'],
                SOURCE_BASE_TABLE=table['SOURCE_BASE_TABLE'],
                RESOLUTION=table['AGGR_TYPE'])        
        else:
            sqlplus_script="""
            INSERT INTO PMMCONF_DB.SMA_KPI_DATE_CONTROL VALUES
            ('{SMA_NAME}','{SOURCE_BASE_TABLE}','{RESOLUTION}',sysdate)
            """.format(
                SMA_NAME=table['SMA_NAME'],
                SOURCE_BASE_TABLE=table['SOURCE_BASE_TABLE'],
                RESOLUTION=table['AGGR_TYPE'])
        try:
            cursor.execute(sqlplus_script)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(table['SMA_NAME']+" --- "+str(e)+" --- "+sqlplus_script.replace('\n',' ')	)
            return False
        db.commit()


def query_and_load_data(table):
    """
    checks the datetime and devices to be loaded, removes them from the target table and inserts them again
    """

    if table['DESTINATION_USER_ID']:
        DB_USER_T=table['DESTINATION_USER_ID']
        DB_PASSWORD_T=base64.b64decode(table['DESTINATION_PW'])
        ORACLE_SID_T=table['DESTINATION_DB']
        DB_HOST_T=table['DESTINATION_DB_HOST']
    else:
        DB_USER_T=DB_USER
        DB_PASSWORD_T=DB_PASSWORD
        ORACLE_SID_T=ORACLE_SID
        DB_HOST_T=DB_HOST

    with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db_s, ManagedDbConnection(DB_USER_T,DB_PASSWORD_T,ORACLE_SID_T,DB_HOST_T) as db:
        # print (table['DESTINATION_DB'])
        cursor=db.cursor()
        cursor_s=db_s.cursor()

        last_handled_datestamp=get_last_handled_datestamp(table)
        update=True
        if not last_handled_datestamp:
            #KPI was never handled use max_history_mins to start
            max_history_mins=filter(lambda x:x['name']==table['AGGR_TYPE'],RESOLUTIONS)[0]['max_history_mins']
            last_handled_datestamp=(datetime.datetime.now() - datetime.timedelta(minutes = max_history_mins)).strftime("%d-%b-%y %H:%M")
            app_logger.info("{SMA_NAME} {SOURCE_BASE_TABLE} {RESOLUTION} was never handled starting from {last_handled_datestamp}".format(SMA_NAME=table['SMA_NAME'],SOURCE_BASE_TABLE=table['SOURCE_BASE_TABLE'],RESOLUTION=table['AGGR_TYPE'],last_handled_datestamp=last_handled_datestamp))
            update=False

        app_logger.info('Processing {sma} {SOURCE_BASE_TABLE} {RESOLUTION} from {last_handled_datestamp}'.format(SOURCE_BASE_TABLE=table['SOURCE_BASE_TABLE'],sma=table['SMA_NAME'],RESOLUTION=table['AGGR_TYPE'],last_handled_datestamp=last_handled_datestamp))


        kpi_list=[]
        for kpi in table['KPI_LIST']:
            kpi_list.append("'"+kpi['KPI_NAME']+"'")
            kpi_list.append(kpi['FORMULA']+'('+kpi['KPI_EXPRESSION']+')')
            kpi_list.append("'"+kpi['UNITS']+"'")
        kpi_list=",\n".join(kpi_list)

        SMA_ADDITIONAL_CRITERIA=""
        if table['SMA_ADDITIONAL_CRITERIA']:
            SMA_ADDITIONAL_CRITERIA=" AND {SMA_ADDITIONAL_CRITERIA}".format(SMA_ADDITIONAL_CRITERIA=table['SMA_ADDITIONAL_CRITERIA'])

        KPI_ADDITIONAL_CRITERIA=""
        if table['KPI_ADDITIONAL_CRITERIA']:
            KPI_ADDITIONAL_CRITERIA=" AND {KPI_ADDITIONAL_CRITERIA}".format(KPI_ADDITIONAL_CRITERIA=table['KPI_ADDITIONAL_CRITERIA'])            

        DEVICE_CRITERIA=""
        if table['DEVICE_CRITERIA']:
            DEVICE_CRITERIA=" AND {DEVICE_CRITERIA}".format(
                DEVICE_CRITERIA=table['DEVICE_CRITERIA']
                )

        DEVICE_LIST=""
        if table['DEVICE_LIST']:
            DEVICE_LIST=" AND {DEVICE_FIELD_NAME} in ({DEVICE_LIST})".format(
                DEVICE_FIELD_NAME=table['F_DEVICE_FIELD_NAME'],
                DEVICE_LIST=table['DEVICE_LIST']
                )

        DATETIME_OFFSET=2
        if table['SOURCE_RESOLUTION']=='5M' or table['SOURCE_RESOLUTION']=='15M':
            DATETIME_OFFSET=2
        elif table['SOURCE_RESOLUTION']=='HR':
            DATETIME_OFFSET=7
        elif table['SOURCE_RESOLUTION']=='DY':
            DATETIME_OFFSET=30
        elif table['SOURCE_RESOLUTION']=='WK':
            DATETIME_OFFSET=90
        elif table['SOURCE_RESOLUTION']=='MO':
            DATETIME_OFFSET=180
        #get the datetimes created after last_handled_datestamp
        sqlplus_script="""    
        SELECT DISTINCT TO_CHAR({DATETIME},'DD-MON-YY HH24:MI'),{DEVICE_FIELD_NAME}
        FROM {SOURCE_BASE_TABLE}_{SOURCE_RESOLUTION}
        WHERE DATETIME_INS>TO_DATE('{last_handled_datestamp}','DD-MON-YY HH24:MI') 
        AND DATETIME > TO_DATE('{last_handled_datestamp}','DD-MON-YY HH24:MI')-{DATETIME_OFFSET}
        {SMA_ADDITIONAL_CRITERIA} 
        {KPI_ADDITIONAL_CRITERIA} 
        {DEVICE_CRITERIA} 
        {DEVICE_LIST}
        """.format(        
            DEVICE_FIELD_NAME=table['F_DEVICE_FIELD_NAME'],
            SOURCE_BASE_TABLE=table['SOURCE_BASE_TABLE'],
            SOURCE_RESOLUTION=table['SOURCE_RESOLUTION'],
            last_handled_datestamp=last_handled_datestamp,
            SMA_ADDITIONAL_CRITERIA=SMA_ADDITIONAL_CRITERIA,
            KPI_ADDITIONAL_CRITERIA=KPI_ADDITIONAL_CRITERIA,
            DEVICE_CRITERIA=DEVICE_CRITERIA,
            DEVICE_LIST=DEVICE_LIST,
            DATETIME_OFFSET=DATETIME_OFFSET,
            DATETIME=table['DATETIME'],
            )

        # print(sqlplus_script)
        try:
            cursor_s.execute(sqlplus_script)
        except cx_Oracle.DatabaseError as e:
            app_logger.error(table['SMA_NAME']+" --- "+str(e)+" --- "+sqlplus_script.replace('\n',' '))
            return False
        source_data_keys=cursor_s.fetchall()
        if source_data_keys:
            #Delete Data before creating and loading the bcp
            KPI_LIST=','.join(map(lambda x: "'"+x['KPI_NAME']+"'", table['KPI_LIST']))
            KPI_LIST_WHERE="AND (KPI_NAME in ({KPI_LIST})) AND AGG_TYPE='{AGG_TYPE}'".format(
                KPI_LIST=KPI_LIST,AGG_TYPE=table['AGGR_TYPE'])
            WHERE_DELETE=""
            index=0
            for line in source_data_keys:
                if index > 0:
                    WHERE_DELETE+="\nOR "
                else:
                    WHERE_DELETE+="("
                WHERE_DELETE+="(DATETIME=TO_DATE('{DATETIME}','DD-MON-YY HH24:MI') AND {DEVICE_TARGET_FIELD}='{DEVICE_NAME}')".format(DATETIME=line[0],
                    DEVICE_NAME=line[1],
                    DEVICE_TARGET_FIELD=table['DEVICE_TARGET_FIELD'].split(',')[0])
                if index == 10:
                    WHERE_DELETE+=")" 
                    
                    sqlplus_script="""                
                    DELETE FROM {DESTINATION_TABLE} WHERE {WHERE_DELETE} {KPI_LIST_WHERE}
                    """.format(
                        DESTINATION_TABLE=table['DESTINATION_TABLE'],
                        WHERE_DELETE=WHERE_DELETE,
                        KPI_LIST_WHERE=KPI_LIST_WHERE,
                        )
                    try:
                        cursor.execute(sqlplus_script)
                    except cx_Oracle.DatabaseError as e:
                        app_logger.error(table['SMA_NAME']+" --- "+str(e)+" --- "+sqlplus_script.replace('\n',' '))
                        return False
                    db.commit()
                    index=0
                    WHERE_DELETE=""
                else:
                    index+=1

            if index > 0:
                WHERE_DELETE+=")"                 
                sqlplus_script="""                
                DELETE FROM {DESTINATION_TABLE} WHERE {WHERE_DELETE} {KPI_LIST_WHERE}
                """.format(
                    DESTINATION_TABLE=table['DESTINATION_TABLE'],
                    WHERE_DELETE=WHERE_DELETE,
                    KPI_LIST_WHERE=KPI_LIST_WHERE,
                    )
                try:
                    cursor.execute(sqlplus_script)
                except cx_Oracle.DatabaseError as e:
                    app_logger.error(table['SMA_NAME']+" --- "+str(e)+" --- "+sqlplus_script.replace('\n',' '))
                    return False
                db.commit()
            
            
            sqlplus_script="""            
            SELECT TO_CHAR( {DATETIME} ,'DD-MON-YY HH24:MI'),
            '{AGGR_TYPE}',
            {DEVICE_FIELD_NAME},
            TO_CHAR(SYSDATE,'DD-MON-YY HH24:MI'),
            {kpi_list}
            FROM {SOURCE_BASE_TABLE}_{SOURCE_RESOLUTION}
            WHERE DATETIME_INS>TO_DATE('{last_handled_datestamp}','DD-MON-YY HH24:MI') 
            AND DATETIME > TO_DATE('{last_handled_datestamp}','DD-MON-YY HH24:MI')-2
            {SMA_ADDITIONAL_CRITERIA} {KPI_ADDITIONAL_CRITERIA} {DEVICE_CRITERIA} {DEVICE_LIST}
            GROUP BY {DATETIME},{DEVICE_FIELD_NAME}
            """.format(            
                AGGR_TYPE=table['AGGR_TYPE'],
                DEVICE_FIELD_NAME=table['DEVICE_FIELD_NAME'],
                DATETIME=table['DATETIME'],
                kpi_list=kpi_list,
                SOURCE_BASE_TABLE=table['SOURCE_BASE_TABLE'],
                SOURCE_RESOLUTION=table['SOURCE_RESOLUTION'],
                SMA_ADDITIONAL_CRITERIA=SMA_ADDITIONAL_CRITERIA,
                KPI_ADDITIONAL_CRITERIA=KPI_ADDITIONAL_CRITERIA,
                DEVICE_CRITERIA=DEVICE_CRITERIA,
                last_handled_datestamp=last_handled_datestamp,
                DEVICE_LIST=DEVICE_LIST
                )
            #print(sqlplus_script)
            try:
                cursor_s.execute(sqlplus_script)
            except cx_Oracle.DatabaseError as e:
                app_logger.error(table['SMA_NAME']+" --- "+str(e)+" --- "+sqlplus_script.replace('\n',' '))
                return False
            
            #Build bcp file
            file_name="{BCP_IN_DIR}/{SMA_NAME}-{SOURCE_BASE_TABLE}_{SOURCE_RESOLUTION}_{DATETIME}-{DESTINATION_TABLE}".format(
                BCP_IN_DIR=BCP_IN_DIR,
                SMA_NAME=table['SMA_NAME'],
                SOURCE_BASE_TABLE=table['SOURCE_BASE_TABLE'],
                SOURCE_RESOLUTION=table['SOURCE_RESOLUTION'],
                DATETIME=datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S"),
                DESTINATION_TABLE=table['DESTINATION_TABLE'])
            base_len=3+len(table['DEVICE_TARGET_FIELD'].split(','))
            with open(file_name,'w') as file:
                for record in cursor_s:
                    for i in range(base_len,len(record),3):
                        file.write(','.join(['' if x is None else x for x in record[:base_len]])+',')
                        file.write(str(record[i])+','+str(record[i+1])+','+str(record[i+2])+'\n')
                try:
                    os.rename(file_name, file_name+".bcp")                
                    file_name+=".bcp"
                    app_logger.info(table['SMA_NAME']+" --- "+'File {file_name} was created'.format(file_name=file_name))
                except OSError:
                    pass
        else:
            app_logger.info('No records found for SMA {SMA_NAME} {SOURCE_BASE_TABLE}_{SOURCE_RESOLUTION} SQL: {sqlplus_script}'.format(SOURCE_BASE_TABLE=table['SOURCE_BASE_TABLE'],
            SOURCE_RESOLUTION=table['SOURCE_RESOLUTION'],
            SMA_NAME=table['SMA_NAME'],
            sqlplus_script=sqlplus_script.replace('\n',' ')
            ))
        #Update LAST_HANDLED_DATESTAMP
        update_last_handled_datestamp(table,update)

def load_bcp_files():
    while True:
        file_list=glob.glob(BCP_IN_DIR+"/*bcp")
        for file_name in file_list:
            target_table=file_name.replace('.bcp','').split('-')[-1]
            sma_name=os.path.basename(file_name).split('-')[0]
            sma=filter(lambda x: x['SMA_NAME']==sma_name,sma_details)
            if sma:
                sma=sma[0]
            else:
                app_logger.error('SMA not found in file {file_name}'.format(file_name=file_name))
                os.rename(file_name, BCP_DONE_DIR+'/'+os.path.basename(file_name))
                continue

            kpi=filter(lambda x: x['SMA_NAME']==sma_name,kpi_details)
            if kpi:
                kpi=kpi[0]
            else:
                app_logger.error('SMA not found in file {file_name}'.format(file_name=file_name))
                os.rename(file_name, BCP_DONE_DIR+'/'+os.path.basename(file_name))
                continue

            ctl_file=TMP_DIR+'/'+target_table+'.ctl'
            with open(ctl_file,'w') as file:
                file.write('load data\n')
                file.write("INFILE '{file_name}'\n".format(file_name=file_name))
                file.write('INTO TABLE {target_table}\n'.format(target_table=target_table))
                file.write('APPEND\n')
                file.write("FIELDS TERMINATED BY ','\n")
                file.write('(DATETIME DATE "DD-MON-YY HH24:MI",\n')
                file.write('AGG_TYPE  ,\n')
                file.write('{DEVICE_TARGET_FIELD} ,\n'.format(DEVICE_TARGET_FIELD=kpi['DEVICE_TARGET_FIELD']))
                file.write('REC_CREATE_DATE DATE "DD-MON-YY HH24:MI",\n')
                file.write('KPI_NAME,\n')
                file.write('KPI_VALUE,\n')
                file.write('KPI_UNITS )\n')
            app_logger.info("{sma_name} --- Loading {file_name}".format(sma_name=sma_name,file_name=file_name))
            log_file=LOG_DIR+'/'+os.path.basename(file_name.replace('.bcp','.log'))
            DESTINATION_PW=None
            if sma['DESTINATION_PW']:
                DESTINATION_USER_ID=sma['DESTINATION_USER_ID']
                DESTINATION_PW=base64.b64decode(sma['DESTINATION_PW'])
                DESTINATION_DB=sma['DESTINATION_DB']
            else:
                DESTINATION_USER_ID=DB_USER
                DESTINATION_PW=DB_PASSWORD
                DESTINATION_DB=ORACLE_SID
            try:
                returncode,sqlldr_out=run_sqlldr(ctl_file,
                    log_file,
                    DESTINATION_USER_ID,
                    DESTINATION_PW,
                    DESTINATION_DB,
                )
                if returncode==0:
                    os.remove(log_file)
                os.remove(ctl_file)
            except OSError:
                pass	
            app_logger.info("{sma_name} --- Moving file {file_name} to {BCP_DONE_DIR}".format(sma_name=sma_name,BCP_DONE_DIR=BCP_DONE_DIR,file_name=file_name))
            os.rename(file_name, BCP_DONE_DIR+'/'+os.path.basename(file_name))
        time.sleep(10)

def process_queue(q):
    """
    Process the kpis in processes_queue
    """
    while True:
        table=q.get()
        query_and_load_data(table)
        q.task_done()

def check_running():
	process_name=os.path.basename(sys.argv[0])
	pids=[pid for pid in os.listdir('/proc') if pid.isdigit()]
	for pid in pids:
		if int(pid) == int(os.getpid()):
			continue
		try:
			cmd=open(os.path.join('/proc',pid,'cmdline')).read()
			if process_name in cmd and 'python' in cmd:
				app_logger.error('Already running {pid} {cmd}'.format(pid=pid, cmd=cmd))
				quit()
		except IOError:
			continue

app_logger=get_logger('cognos')
app_logger.info('Starting Cognos Process')

#Check that the process is not running
check_running()

workers=[]

#Load Metadata
load_metada()
worker = Thread(target=th_load_metada, args=())
worker.setDaemon(True)
worker.start()
workers.append({'function':th_load_metada,'params':'','object':worker})

#Trigger threads to handle every resolution
for resolution in RESOLUTIONS:
    worker = Thread(target=th_enqueue, args=(resolution,))
    worker.setDaemon(True)
    workers.append({'function':th_enqueue,'params':resolution,'object':worker})
    worker.start()

#thread to listen for run now kpi's and add them to the queue
worker = Thread(target=th_run_now_kpi, args=())
worker.setDaemon(True)
workers.append({'function':th_run_now_kpi,'params':'','object':worker})
worker.start()


#Trigger thread to generate the bcp files
for i in range(threads):
    worker = Thread(target=process_queue, args=(processes_queue,))
    worker.setDaemon(True)
    workers.append({'function':process_queue,'params':processes_queue,'object':worker})
    worker.start()

#Trigger the sqlldr thread
try:
    worker = Thread(target=load_bcp_files, args=())
    worker.setDaemon(True)
    workers.append({'function':load_bcp_files,'params':'','object':worker})
    worker.start()
except Exception as e:
    logger.exception("load_bcp_files crashed. Error: %s", e)

#Monitor that none of the threads crashes
while True:
    for idx,running_worker in enumerate(workers):
        if not running_worker['object'].isAlive():
            app_logger.error('Thread {running_worker} crashed running it again'.format(running_worker=running_worker))
            if running_worker['params']:
                worker = Thread(target=running_worker['function'], args=(running_worker['params'],))
            else:
                worker = Thread(target=running_worker['function'], args=())
            worker.setDaemon(True)
            workers[idx]['object']=worker
            worker.start()
    time.sleep(900)

