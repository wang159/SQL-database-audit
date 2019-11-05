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

from main_foreign_key_match import main_foreign_key_match
from class_custom_tables import *




  
def get_dict_from_db2(inparam, db_name, table_name, column_name_list, index_column_name_list):
  engine = create_engine('mysql+pymysql://'+inparam['DB2_SQL_CRED_USR']+':'+inparam['DB2_SQL_CRED_PSW'] \
                                         +'@'+inparam['DB2_SQL_addr']+':'+inparam['DB2_SQL_port']+'/'+db_name)  

  code.interact(local=locals())
  
  if not database_exists(db2_nanohub_engine.url):
    logging.error('DB2 Database '+str(db2_nanohub_engine.url)+' does not exist! Please create it first.')

  connection = db2_nanohub_engine.connect()
  metadata = db.MetaData()

  if table_name == 'jos_users':
  # id, name, username, registerDate, lastvisitDate
    sql_table = db.Table(table_name, metadata,
         db.Column('id', db.Integer, primary_key=True),
         db.Column('name', db.String(255)),
         db.Column('username', db.String(255)),
         db.Column('email', db.String(255)),
         db.Column('registerDate', db.DateTime),
         db.Column('lastvisitDate', db.DateTime)
      )
    query = db.select([sql_table.c.id, sql_table.c.name, sql_table.c.username, sql_table.c.email, \
                     sql_table.c.registerDate, sql_table.c.lastvisitDate])
    
  else:
    sql_table = db.Table(table_name, metadata, autoload=True, autoload_with=db2_nanohub_engine)
    query = db.select([sql_table])
      
  result_proxy = connection.execute(query)
  result_set = result_proxy.fetchall()

  # form a dictionary
  this_dict = dict()

  for this_index_column_name in index_column_name_list:
    for this_result in result_set:
      this_key = this_result.__getitem__(this_index_column_name)
      
      if not this_key:
        continue
        
      if not isinstance(this_key, int):
        # key is always all lower-case or numeric
        this_key = this_key.lower()
        
      this_dict[this_key] = dict()    

      for this_column_name in column_name_list:
        this_dict[this_key][this_column_name] = this_result.__getitem__(this_column_name)


  return this_dict
    
  
  
  
def main_sql_audit(inparam, db_name, db_params):

  ### Load some DB2 support tables into dict
  #   Due to much information are embedded in unstructured data such as URL in web,
  #   we cannot do a simple join of tables in SQL. Instead, we have to look information up
  #   individually
  #support_table_dict = dict()

  # TABLE: email
  #support_table_dict['bc_users_id_username_email_dict'] = get_dict_from_db2(inparam, 'nanohub', 'jos_users', ['email', 'id'], ['id', 'username'])  
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

  run_option = '1: schema audit'; # options '1: schema audit', '2: foreign key match', '3: activity monitor'
  #run_option = '2: foreign key match'; # options '1: schema audit', '2: foreign key match', '3: activity monitor'
  #run_option = '3: activity monitor'; # options '1: schema audit', '2: foreign key match', '3: activity monitor'
  
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
  
  db_params['sampling'] = -1
  
  db_params['special_table'] = dict()
  db_params['special_table']['nanohub'] = dict()
  db_params['special_table']['nanohub']['jos_users'] = table_jos_users()

  db_params['exclude_table'] = dict()
  db_params['exclude_table']['narwhal'] = ['backup_joblog_20180703']

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
  elif run_option == '3: activity monitor':
    pass
  
  
  
  
  

