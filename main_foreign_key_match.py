from class_sql_auditor import *

from pprint import pprint
import glob
import os

from functools import reduce
import copy

import pickle
import json


def map_match_foreign_keys(this_filepath, all_filepath_list):
  
  # find possible foreign key candidates for table stored in this_filepath to all tables in all_filepath_list
  # For all primary and unique keys in this_filepath, find all matchable foreign key candidates in all_filepath_list
  
  foreign_key_map = dict()
  
  with open(this_filepath, 'rb') as fid:
    table_auditor = pickle.load(fid)
    column_dv = table_auditor.column_distinct_values

  if table_auditor.column_row_cnt[list(table_auditor.column_row_cnt.keys())[0]] == 0:
    # empty table
    return
  
  foreign_key_map[table_auditor.table_name] = dict()
  
  # compare each of this table's column with target
  this_pk_list = table_auditor.pk
  if len(this_pk_list) > 1:
    # multiple primary keys present. 
    this_pk_list = []
    
  for this_pk in (this_pk_list+table_auditor.uk):
  
    # this set of distinct values for a column
    this_dv = set(filter(None, column_dv[this_pk]))
    if not this_dv:
      # skip all empty columns
      continue 
              
    for this_target_filepath in all_filepath_list:

      # for each target table
      
      if this_target_filepath == this_filepath:
        continue
      
      with open(this_target_filepath, 'rb') as fid:
        target_table_auditor = pickle.load(fid)
        target_column_dv = target_table_auditor.column_distinct_values  
      
      if target_table_auditor.column_row_cnt[list(target_table_auditor.column_row_cnt.keys())[0]] == 0:
        # empty table
        continue 

      possible_keys = list()
      
      for this_target_key in target_column_dv:
        this_target_pk_list = target_table_auditor.pk
        if len(this_target_pk_list) > 1:
          # multiple primary keys present. No need to avoid these keys
          this_target_pk_list = []
        if this_target_key in (target_table_auditor.pk+target_table_auditor.uk):
          # skip if this column is a unique key
          continue
          
        # CHECK: if name matches
        if this_target_key == this_pk:
          # column names exact match
          name_exact_matched = True
                
        this_target_dv = set(filter(None, target_column_dv[this_target_key]))
        if not this_target_dv:
          # skip all empty columns
          continue
        
        unique_intersect = this_dv.intersection(this_target_dv)
        
        # see if the unique intersect is whole of any dv (distinct values)
        this_dv_intersect_diff = len(this_dv) - len(unique_intersect)
        this_target_dv_intersect_diff = len(this_target_dv) - len(unique_intersect)

        if this_target_dv_intersect_diff == 0:
          # eligible as foreign key
          if this_target_key == this_pk:
            # column names exact match
            possible_keys.append((100, this_target_key)) # 100% confidence
          elif not isinstance(list(this_target_dv)[0], int):
            # non-numeric match
            possible_keys.append((100, this_target_key)) # 100% confidence
          else:
            possible_keys.append(((1-this_dv_intersect_diff/len(this_dv))*100.0/2.0, this_target_key)) # max 50% confidence
        
      if possible_keys:
        pprint('------------- Possible foreign key pairs: ')
        pprint(table_auditor.table_name+' : '+this_pk)
        
        foreign_key_map[table_auditor.table_name][target_table_auditor.table_name] = list()
        for this_tuple in sorted(possible_keys, key = lambda x: x[0]):
          this_target_score = round(list(this_tuple)[0])
          this_target_col = list(this_tuple)[1]
          pprint(target_table_auditor.table_name+' : '+ str(round(list(this_tuple)[0]))+' '+str(list(this_tuple)[1]))
          foreign_key_map[table_auditor.table_name][target_table_auditor.table_name].append([this_pk, this_target_col])

          
    
  
  return foreign_key_map




def reduce_match_foreign_keys(key_map_1, key_map_2):
  # key_map_1[table_1][table_2] = [[t1_key_1, t2_key_1], [t1_key_2, t2_key_2], ....]
  if key_map_1 == None:
    key_map_1 = dict()
  if key_map_2 == None:
    key_map_2 = dict()
    

  key_map_1.update(key_map_2)
  key_map = copy.deepcopy(key_map_1)
  
  # make sure both A->B and B->A connections are present
  for this_key_1 in key_map_1:
    for this_key_2 in key_map_1[this_key_1]:
      for this_connect in key_map_1[this_key_1][this_key_2]:

        if this_key_2 not in key_map:
          key_map[this_key_2] = dict()
          
        if this_key_1 not in key_map[this_key_2]:
          key_map[this_key_2][this_key_1] = list()
        
        this_connect.reverse()
        if this_connect not in key_map[this_key_2][this_key_1]:
          #pprint(this_connect)
          #pprint('to '+this_key_2+', '+this_key_1)
          key_map[this_key_2][this_key_1].append(this_connect)
  
  return key_map






def main_foreign_key_match(inparam, db_name, db_params):
  
  # collect all valid pickle file paths

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
    mapped_key_list = map(map_match_foreign_keys, all_filepath_list, [all_filepath_list for x in range(0,len(all_filepath_list))])
    foreign_keys_dict = reduce(reduce_match_foreign_keys, mapped_key_list)
    
    len(foreign_keys_dict) # used to trigger the lazy evaluation of map() and reduce()
  
  
  # output the key information into readable JSON file
  with open(os.path.join(db_params['pickle_dir'], 'sql_column_connect_map.json'), 'w') as fid:
 
    json.dump(foreign_keys_dict, fid, indent = 2)
  










