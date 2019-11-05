from class_sql_auditor import *

from pprint import pprint
import glob
import os

import pickle


def map_match_foreign_keys(this_filepath, all_filepath_list):
  
  # find possible foreign key candidates for table stored in this_filepath to all tables in all_filepath_list

  with open(this_filepath, 'rb') as fid:
    table_auditor = pickle.load(fid)
    column_dv = table_auditor.column_distinct_values
    
  for this_target_filepath in all_filepath_list:
    
    # for each target table
    
    if this_target_filepath == this_filepath:
      continue
    
    with open(this_target_filepath, 'rb') as fid:
      target_table_auditor = pickle.load(fid)
      target_column_dv = target_table_auditor.column_distinct_values  
    
    # with both this worker's table and target table loaded, compare column by column
    for this_key in column_dv:
      this_dv = set(column_dv[this_key])
      
      for this_target_key in target_column_dv:
        this_target_dv = set(target_column_dv[this_target_key])
        
        unique_intersect = this_dv.intersection(this_target_dv)
        
        # see if the unique intersect is whole of any dv (distinct values)
        this_dv_intersect_diff = len(unique_intersect) - len(this_dv)
        this_target_dv_intersect_diff = len(unique_intersect) - len(this_target_dv)
        
        if (this_dv_intersect_diff == 0) | (this_target_dv_intersect_diff == 0):
          pprint('Possible foreign key pairs: ')
          pprint(table_auditor.table_name+' : '+this_key)
          pprint(target_table_auditor.table_name+' : '+this_target_key)
          pprint(this_dv_intersect_diff)
          pprint(this_target_dv_intersect_diff)
    
  
  code.interact(local=locals())
  return 1




def main_foreign_key_match(inparam, db_name, db_params):
  
  # collect all valid pickle file paths
  db_params['pickle_dir'] = '/scratch/halstead/w/wang159/db2'
  
  all_filepath_list = list()
  for this_filepath in glob.glob(os.path.join(db_params['pickle_dir'], db_name, '*.p')):
    #pprint(this_filepath)
    all_filepath_list.append(this_filepath)
  
  
  
  # dispatch all files to Spark for foreign key matching
  
  if inparam['use_spark']:
    # use SPARK
    pass
  else:
    # do not use SPARK
    pprint('Running WITHOUT Spark support ...')
    result = map(map_match_foreign_keys, all_filepath_list, [all_filepath_list for x in range(0,len(all_filepath_list))])
    list(result) # used to trigger the lazy evaluation of map()
