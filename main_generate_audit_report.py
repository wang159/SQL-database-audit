import json
import os

def main_generate_audit_report(inparam, db_name, db_params):
  #db_params['pickle_dir'] = '/scratch/halstead/w/wang159/db2'

  ### Generate D3 tree-compatible JSON
  # load data
  with open(os.path.join(db_params['pickle_dir'], 'all_table_info.json'), 'r') as fid:
    all_table_info = json.load(fid)
  
  d3_schema_data = dict()
  for this_table in all_table_info:

    # for each table
    '''
    { 
      "name": "Level 2: B",
      "children": [
      { "name": "Son of A" },
      { "name": "Daughter of A" }
      ]
    }
    '''
    d3_schema_data[this_table] = {'name':this_table, 'children':[]}
    
    for this_col_name in all_table_info[this_table]['column_names']:
      d3_schema_data[this_table]['children'].append({'name':this_col_name})
        
  with open(os.path.join(db_params['d3_data_dir'], 'd3_schema_data.json'), 'w') as fid:
    json.dump(d3_schema_data, fid, indent=4)  
      
  ### Generate D3 connectivity JSON
  '''
  {
  "name": "Hidden root",
  "sample": {"test":[1,2,3,4,5]},
  "children":[
      {
      "name": "Top Level",

      "children": [
          { 
            "name": "Level 2: A",
            "children": [
              { "name": "Son of A"},
              { "name": "Daughter of A"}
            ]
          },
      ......
  '''
     
  # load data
  with open(os.path.join(db_params['pickle_dir'], 'sql_column_connect_map.json'), 'r') as fid:
    connect_map = json.load(fid)
  
  d3_connect_data = dict()
  for this_table in connect_map:

    # for each table
    d3_connect_data[this_table] = dict()
    
    for this_target_table in connect_map[this_table]:

      # for each table->target_table connection          
      for this_connect in connect_map[this_table][this_target_table]:
        
        # for each connection ['col_name_1', 'col_name_2']
        if this_connect[0] not in d3_connect_data[this_table]:
          d3_connect_data[this_table][this_connect[0]] = dict()
        if this_target_table not in d3_connect_data[this_table][this_connect[0]]:
          d3_connect_data[this_table][this_connect[0]][this_target_table] = list()
          
        d3_connect_data[this_table][this_connect[0]][this_target_table].append(this_connect[1])
        
  with open(os.path.join(db_params['d3_data_dir'], 'd3_connect_data.json'), 'w') as fid:
    json.dump(d3_connect_data, fid, indent=4)    
        
        
        
        
        

