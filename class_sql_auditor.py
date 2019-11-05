import sqlalchemy as db
from sqlalchemy import inspect
from sqlalchemy.engine.reflection import Inspector

import code

import pickle
import os

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
    
    # audit each table
    for this_table_auditor in self.table_auditor_list:
      # audit this table
      this_table_auditor.audit_schema()
    
      # pickle this table auditor into pre-defined output directory
      pickle_filepath = os.path.join(self.params['pickle_dir'], self.database, this_table_auditor.table_name+'.p')
      pprint(pickle_filepath)
      with open(pickle_filepath, 'wb') as fid:
        this_table_auditor.engine = None
        pickle.dump(this_table_auditor, fid)    
    
    







    
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

    pprint('Primary keys are: ')
    pprint(self.pk)
    pprint('Unique keys are: ')
    pprint(self.uk)
    
    
    # find distinct values of each column
    self.find_distinct_value()
    
    
    
  def find_distinct_value(self):
    
    # find distinct values of each column. Only for ones eligible as 'foreign_key_types'
    column_keys = self.sql_table.columns.keys()
    connection = self.engine.connect()
         
    for this_column_key in column_keys:
      this_column = self.sql_table.columns[this_column_key]
      
      if type(this_column.type) not in self.params['foreign_key_types']:
        pprint(this_column)
        continue
        
      query = db.select([this_column])

      if self.params['sampling'] > 0:
        result_proxy = connection.execute(query.limit(self.params['sampling']).distinct())
      else:
        result_proxy = connection.execute(query.distinct())
                
      result_set = result_proxy.fetchall()
    
      self.column_distinct_values[this_column_key] = [x.values()[0] for x in result_set]
    
