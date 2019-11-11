import sqlalchemy as db
from sqlalchemy import inspect, desc, asc
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.expression import func, select
from sqlalchemy.orm import sessionmaker

import code

import pickle
import os
import random
import json

from pprint import pprint


class SQL_db_auditor():
  
  def __init__(self, engine, params):
    # engine from sqlalchemy
    self.engine = engine
    self.params = params
    self.database = self.engine.url.database
    
    # list of table-level auditors
    self.table_auditor_list = list()
    
    self.refresh(params)
  
  
  
  def refresh(self, params):
    # refresh information about the DB
    # destroy old table objects and spawn new ones
    # Also checks accessibility of each table.
    # Best to do this before a new round of audit
    
    # update key DB parameters
    inspector = inspect(self.engine)
    
    self.table_name_list = inspector.get_table_names()

    for this_table_name in self.table_name_list:
      pprint('Auditing table: '+this_table_name)
      
      # skip if this is on exclude list
      if self.database in params['exclude_table']:
        if this_table_name in params['exclude_table'][self.database]:
          pprint('   >> Excluded')
          continue
      
      this_table_auditor = SQL_table_auditor(self.engine, self.params, this_table_name)
      self.table_auditor_list.append(this_table_auditor)
  
  
    
  def audit_and_record_all_table_auditors(self):
    
    database_info = dict() # collecting all self.info from each table auditor
    
    # audit each table
    for this_table_auditor in self.table_auditor_list:
      # audit this table
      this_table_auditor.audit_schema()
    
      pickle_db_dir = os.path.join(self.params['pickle_dir'], self.database)
            
      # pickle this table auditor into pre-defined output directory
      if not os.path.exists(pickle_db_dir):
        os.makedirs(pickle_db_dir)
        
      pickle_filepath = os.path.join(pickle_db_dir, this_table_auditor.table_name+'.p')

      with open(pickle_filepath, 'wb') as fid:
        this_table_auditor.engine = None
        this_table_auditor.session = None
        pickle.dump(this_table_auditor, fid)    
    
      pprint('Save table info: ' + pickle_filepath)
      
      # collect general information from self.info
      database_info[this_table_auditor.table_name] = this_table_auditor.info
      


    # output general information JSON file
    with open(os.path.join(self.params['d3_data_dir'], 'all_table_info.json'), 'w') as fid:
      json.dump(database_info, fid, indent=4)





    
class SQL_table_auditor():

  def __init__(self, engine, params, table_name):
    
    self.engine = engine

    self.table_name = table_name
    self.params = params
    
    self.sql_table = None
    self.pk = list()
    self.uk = list()
    
    
    # table related information
    self.column_distinct_values = dict()
    self.column_row_cnt = dict() # the number of rows at time of fetching SELECT for a column. Therefore, different column
                                 # may have slightly different number of rows
    
    self.info = dict() # general information about the table
  
  
  
  
  
  
  def audit_schema(self):

    # sometimes, user may not have full access to entire table. For these, params['special_table']['table_name']
    # must contain the schema of the table as class sqlalchemy.Table()

    if self.table_name not in self.params['special_table']:
      # full access to table
      self.sql_table = db.Table(self.table_name, db.MetaData(), autoload=True, autoload_with=self.engine)
      
    else:
      # no full access to table
      self.sql_table = self.params['special_table'][self.table_name].__table__
    
    # get primary and unique keys
    insp = Inspector.from_engine(self.engine)
    
    pk_constraint = insp.get_pk_constraint(self.table_name)
    if pk_constraint:
      self.pk = pk_constraint['constrained_columns']
      
    unique_constraints = insp.get_unique_constraints(self.table_name)
    if unique_constraints:
      for this_constraint in unique_constraints:
        self.uk.extend(this_constraint['column_names'])

    print('Primary keys are: ')
    pprint(self.pk)
    print('Unique keys are: ')
    pprint(self.uk)
    
    # find distinct values of each column
    self.find_distinct_value()
    
    # find general table information
    self.find_table_info()
    
    
    
    
    
    
  def find_distinct_value(self):
    
    # find distinct values of each column. Only for ones eligible as 'foreign_key_types'
    column_keys = self.sql_table.columns.keys()
    connection = self.engine.connect()
         
    for this_column_key in column_keys:
      this_column = self.sql_table.columns[this_column_key]
      
      if type(this_column.type) not in self.params['foreign_key_types']:
        continue
        
      query = db.select([this_column])

      if self.params['sampling'] > 0:
        result_proxy = connection.execute(query.limit(self.params['sampling']).distinct())
      else:
        result_proxy = connection.execute(query.distinct())
                
      result_set = result_proxy.fetchall()
    
      self.column_distinct_values[this_column_key] = [x.values()[0] for x in result_set]
      self.column_row_cnt[this_column_key] = len(result_set)
      
    # close connection
    connection.close()
  
  
  
       
  def find_table_info(self):
  
    # output a JSON file containing table information
    json_dict = dict()
    
    ### acquire general information about the table
    Session = sessionmaker(bind=self.engine)
    session = Session()
    
    # total row numbers
    self.info['total_row'] = session.query(self.sql_table).count()
    
    # column names, types
    self.info['column_names'] = list()
    self.info['column_data_types'] = list()
    self.info['column_key_types'] = list()
    
    for this_column_key in self.sql_table.columns.keys():
      this_column = self.sql_table.columns[this_column_key]
      
      self.info['column_names'].append(this_column_key)
      self.info['column_data_types'].append(str(this_column.type))
      
      if this_column.primary_key:
        this_key_type = 'Primary'
      else:
        this_key_type = ''
      
      self.info['column_key_types'].append(this_key_type)
                    
    # randomly sample 5 rows
    #self.info['rand_samples'] = session.query(self.sql_table).order_by(func.rand()).limit(5).all()
    self.info['rand_samples'] = list()
    for this_row in session.query(self.sql_table).order_by(func.rand()).limit(5).all():
      self.info['rand_samples'].append([str(x) for x in this_row])
        
    # unique # of values, range of values, etc.
    self.info['unique_values'] = list()
    self.info['unique_values_count'] = list()
    self.info['unique_values_range'] = list()
    
    for this_column_key in self.sql_table.columns.keys():
      this_column = self.sql_table.columns[this_column_key]
    
      if this_column_key in self.column_distinct_values:
        # this column's distinct values are already calculated
        # pick at most 5 distinct values as example
        self.info['unique_values'].append([str(x) for x in self.column_distinct_values[this_column_key][:5]])
        self.info['unique_values_count'].append(len(self.column_distinct_values[this_column_key]))
        
        distinct_values = self.column_distinct_values[this_column_key]
      
      else:
        # this column is not eligible for distinct values
        self.info['unique_values'].append([])
        self.info['unique_values_count'].append([])
        
        min_var = session.query(this_column).order_by(desc(this_column)).limit(1).all()
        if min_var: min_var = min_var[0]
           
        max_var = session.query(this_column).order_by(asc(this_column)).limit(1).all()
        if max_var: max_var = max_var[0]
        
        distinct_values = [min_var, max_var]

      # determine range of values for Datetime and numeric types
      try:
        self.info['unique_values_range'].append([str(min(distinct_values)), str(max(distinct_values))])
      except:
        self.info['unique_values_range'].append([])
      
    
    session.close()
    #if self.table_name == 'fileperm':
    #  code.interact(local=locals())
    
    
    
