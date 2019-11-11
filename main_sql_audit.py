import sys

import code
import os
from pprint import pprint, pformat
import logging
logging.getLogger().setLevel(logging.INFO)
from glob import glob
import re
import datetime


import sqlalchemy as db
from sqlalchemy import *
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy.orm import sessionmaker

from class_sql_auditor import SQL_db_auditor


from class_custom_tables import *
from main_foreign_key_match import main_foreign_key_match
from main_generate_audit_report import main_generate_audit_report
    
  
  
  
def main_sql_audit(inparam, db_name, db_params):

  engine = create_engine('mysql+pymysql://'+inparam['DB2_SQL_CRED_USR']+':'+inparam['DB2_SQL_CRED_PSW'] \
                                         +'@'+inparam['DB2_SQL_addr']+':'+inparam['DB2_SQL_port']+'/'+db_name)  
  
  sql_db_auditor = SQL_db_auditor(engine, db_params)
  sql_db_auditor.audit_and_record_all_table_auditors()
  
  





def set_param(param, param_name, param_default):

  # check enviorment for param_name. If none, assign param_default
  if param_name.upper() in os.environ:
    # enviormental variable FOUND
    param[param_name] = os.environ.get(param_name.upper())
  else:
    # assign default
    param[param_name] = param_default
    
  return param  


  
if __name__ == '__main__':

  #run_option = '1: schema audit'; # options '1: schema audit', '2: foreign key match', '3: generate report', '4: activity monitor'
  #run_option = '2: foreign key match';
  run_option = '3: generate report';
  #run_option = '4: activity monitor'; 
    
  # Set parameters for SQL database access
  # If not set by enviormental variables, defaults will be used here.
  inparam = dict()
  inparam = set_param(inparam, 'DB2_SQL_CRED_USR', 'set_username_in_env_DB2_SQL_CRED_USR')
  inparam = set_param(inparam, 'DB2_SQL_CRED_PSW', 'set_password_in_env_DB2_SQL_CRED_PSW')
  inparam = set_param(inparam, 'DB2_SQL_addr', '127.0.0.1')
  inparam = set_param(inparam, 'DB2_SQL_port', '3306')
  
  inparam['use_spark'] = False
  
  #db_name = 'nanohub'
  db_name = 'narwhal'

  db_params = dict()
  
  db_params['pickle_dir'] = '/scratch/halstead/w/wang159/db2'
  db_params['d3_data_dir'] = '/home/wang159/nanoHUB/projects/DB2/SQL-database-audit/D3_visualizer'
    
  db_params['sampling'] = -1
  
  db_params['special_table'] = dict()
  db_params['special_table']['nanohub'] = dict()
  db_params['special_table']['nanohub']['jos_users'] = table_jos_users()

  db_params['exclude_table'] = dict()
  db_params['exclude_table']['narwhal'] = ['backup_joblog_20180703']
  
  db_params['exclude_columns']=dict()
  db_params['exclude_columns']['narwhal'] = ['']
  
  '''
  sqlalchemy column data types. From dir(sqlalchemy.dialects.mysql.types)
  
  'BIGINT', 'BIT', 'CHAR', 'DATETIME', 'DECIMAL', 'DOUBLE', 'FLOAT', 'INTEGER', 
  'LONGBLOB', 'LONGTEXT', 'MEDIUMBLOB', 'MEDIUMINT', 'MEDIUMTEXT', 'NCHAR', 'NUMERIC', 
  'NVARCHAR', 'REAL', 'SMALLINT', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYBLOB', 'TINYINT', 
  'TINYTEXT', 'VARCHAR', 'YEAR'
  '''
  
  # forgein_key_types denotes the types that can serve as forgein keys
  db_params['foreign_key_types'] = [ getattr(db.dialects.mysql, x) for x in \
    ['CHAR', 'SMALLINT', 'MEDIUMINT', 'TINYINT', 'BIGINT', 'INTEGER', 'VARCHAR']
  ]
  
  # datetime_types denotes the types can be used as time
  db_params['datetime_types'] = [ getattr(db.dialects.mysql, x) for x in \
    ['DATETIME', 'TIME'] \
  ]
  
  
  if run_option == '1: schema audit':
    main_sql_audit(inparam, db_name, db_params)
    
  elif run_option == '2: foreign key match':
    main_foreign_key_match(inparam, db_name, db_params)
    
  elif run_option == '3: generate report':
    main_generate_audit_report(inparam, db_name, db_params)
    
  elif run_option == '4: activity monitor':
    pass
  
  
  
  
  

