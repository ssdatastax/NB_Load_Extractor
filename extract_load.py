#!/usr/bin/env python3

#pip install xlsxwriter
#pip install Pandas

def sortFunc(e):
  return e['count']

# get param value
def get_param(filepath,param_name,param_pos,ignore="",default_val="Default"):
    if(path.exists(filepath)):
        fileData = open(filepath, "r")
        for line in fileData:
            if(param_name in line):
                if(ignore):
                    if((ignore in line and line.find(ignore)>0) or ignore not in line):
                        default_val = str(line.split()[param_pos].strip())
                else:
                    if(str(line.split()[param_pos].strip())):
                        def_val = str(line.split()[param_pos].strip())
                    return def_val
    return default_val

import os.path
from os import path
import xlsxwriter
import sys

def schemaTag(writeFile,tagType,level,ks,tbl,cql):
  writeFile.writelines(['  - tags:\n','      phase: '+tagType+'_'+level+'_'+ks])
  if level == 'table':
    writeFile.write('_'+tbl)
  writeFile.writelines(['\n','    statements:\n','      - |\n        '+cql+'\n\n'])

def rwTag(writeFile,rwCQL,ks,tbl,tbl_info,ratio='n'):
  if ratio == 'n':
    writeFile.writelines(['  - tags:\n','      phase: '+rwCQL+'_'+ks+'_'+tbl+'\n'])
  elif ratio == 'y':
    writeFile.writelines(['  - tags:\n','      phase: load_'+rwCQL+'_'+ks+'_'+tbl+'\n'])
    ratio_val = str(int(tbl_info['ratio'][rwCQL]*1000))
    writeFile.writelines(['    params:\n','      ratio: ',ratio_val,'\n'])
  writeFile.writelines(['    statements:\n','      - |\n        '])
  field_array = []
  join_info = '},{'+ks+'_'+tbl+'_'
  if rwCQL == 'read':
    cql = 'SELECT * FROM '+ks+'.'+tbl+' WHERE '
    for fld_name,fld_type in tbl_info['field'].items():
      if (fld_name in tbl_info['pk']):
        field_array.append(fld_name+'={'+ks+'_'+tbl+'_'+fld_name+'}')
    field_info = ' AND '.join(map(str, field_array))
    writeFile.write(cql+field_info+'\n\n')
  elif rwCQL == 'write':
    field_array = tbl_info['field'].keys()
    field_names =  ','.join(map(str, field_array))
    field_values =  join_info.join(map(str, field_array))
    cql = 'INSERT INTO '+ks+'.'+tbl+' ('+field_names+') VALUES ({'+ks+'_'+tbl+'_'+field_values+'})'
    writeFile.write(cql+'\n\n')

data_url = []
omit_keyspace = ['OpsCenter','dse_insights_local','solr_admin','test','dse_system','system_auth','system_traces','system','dse_system_local','system_distributed','system_schema','dse_perf','dse_insights','dse_security','killrvideo','dse_leases']
headers=["Keyspace","Table","Reads","% of Total Reads","","Keyspace","Table","Writes","% of Total Writes","","Keyspace","Table","Read % of Incl. Total RW","Write % of Incl. Total RW"]
headers2=["Keyspace","Table","Field","Type","Partition Key","Clustering Column","Small List","Field Length","Data Pattern","Example","Binding Statement"]
read_threshold = .85
write_threshold = .85
include_yaml = 0
new_dc = "'DC1':'3'"
show_help = ''
# new_dc = ''

for argnum,arg in enumerate(sys.argv):
  if(arg=='-h' or arg =='--help'):
    show_help = 'y'
  elif(arg=='-p'):
    data_url.append(sys.argv[argnum+1])
  elif(arg=='-rt'):
    read_threshold = float(sys.argv[argnum+1])/100
  elif(arg=='-wt'):
    write_threshold = float(sys.argv[argnum+1])/100
  elif(arg=='-inc_yaml'):
    include_yaml = 1
  
if show_help:
  help_content = \
  'usage: extract_load.py [-h] [--help] [-inc_yaml]\n'\
  '                       [-p PATH_TO_DIAG_FOLDER]\n'\
  '                       [-rt READ_THRESHOLD]\n'\
  '                       [-wt WRITE_THRESHOLD]\n'\
  'oprional arguments:\n'\
  '-h, --help             This help info\n'\
  '-p                     Path to the diagnostics folder\n'\
  '                        Multiple diag folders accepted\n'\
  '                        i.e. -p PATH1 -p PATH2 -p PATH3\n'\
  '-rt                    Defines percentage of read load\n'\
  '                        to be included in the output\n'\
  '                        Default: 85%\n'\
  '                        i.e. -rt 100\n'\
  '-wt                    Defines percentage of write load\n'\
  '                        to be included in the output\n'\
  '                        Default: 85%\n'\
  '                        i.e. -wt 100\n'\
  '-inc_yaml              Include writing yaml files\n'\
  '                        CLUSTER_NAME_schema.yaml\n'\
  '                         This file is used to create\n'\
  '                         the schema with NoSQLBench\n'\
  '                        CLUSTER_NAME_initial_load.yaml\n'\
  '                         This file is used to create\n'\
  '                         the initial load of read tables\n'\
  '                         with NoSQLBench\n'\
  '                        CLUSTER_NAME_load.yaml\n'\
  '                         This file is used to create\n'\
  '                         ongoing load with NoSQLBench\n'
  exit(help_content)

for cluster_url in data_url:
  is_index = 0
  read_subtotal = 0
  write_subtotal = 0
  total_reads = 0
  total_writes = 0
  count = 0
  read_table = {}
  write_table = {}
  write_table2 = {}
  read_count = []
  write_count = []
  table_totals ={}

  rootPath = cluster_url + "/nodes/"
  for node in os.listdir(rootPath):
    ckpath = rootPath + node + "/nodetool"
    if path.isdir(ckpath):
      iodata = {}
      iodata[node] = {}
      keyspace = ""
      table = ""
      dc = ""
      cfstat = rootPath + node + "/nodetool/cfstats"
      tablestat = rootPath + node + "/nodetool/tablestats"
      try:
        cfstatFile = open(cfstat, "r")
      except:
        cfstatFile = open(tablestat, "r")
      clusterpath = rootPath + node + "/nodetool/describecluster"
      cluster_name = get_param(clusterpath,"Name:",1)
      ks = ''

      for line in cfstatFile:
        line = line.strip('\n').strip()
        if("Keyspace" in line):
          ks = line.split(":")[1].strip()
        if ks not in omit_keyspace and ks != '':
          if("Table: " in line):
            tbl = line.split(":")[1].strip()
            is_index = 0
          elif("Table (index): " in line):
            is_index = 1
          if("Local read count: " in line):
            count = int(line.split(":")[1].strip())
            if (count > 0):
              total_reads += count
              try:
                type(read_table[ks])
              except:
                read_table[ks] = {}
              try:
                type(read_table[ks][tbl])
                read_table[ks][tbl] += count
              except:
                read_table[ks][tbl] = count
          if (is_index == 0):
            if("Local write count: " in line):
              count = int(line.split(":")[1].strip())
              if (count > 0):
                total_writes += count
                try:
                  type(write_table[ks])
                except:
                  write_table[ks] = {}
                try:
                  type(write_table[ks][tbl])
                  write_table[ks][tbl] += count
                except:
                  write_table[ks][tbl] = count

  schema = rootPath + node + "/driver/schema"
  schemaFile = open(schema, "r")
  ks = ""
  tbl = ""
  create_stmt = {}
  tbl_data = {}
  for line in schemaFile:
    line = line.strip('\n').strip()
    if("CREATE KEYSPACE" in line):
      prev_ks = ks
      ks = line.split()[2].strip('"')
      tbl_data[ks] = {'cql':line}
    elif("CREATE INDEX" in line):
      prev_tbl = tbl
      tbl = line.split()[2].strip('"')
      tbl_data[ks][tbl] = {'type':'index', 'cql':line}
    elif("CREATE TYPE" in line):
      prev_tbl = tbl
      tbl_line = line.split()[2].strip()
      tbl = tbl_line.split(".")[1].strip().strip('"')
      tbl_data[ks][tbl] = {'type':'type', 'cql':line}
      tbl_data[ks][tbl]['field'] = {}
    elif("CREATE TABLE" in line):
      prev_tbl = tbl
      tbl_line = line.split()[2].strip()
      tbl = tbl_line.split(".")[1].strip().strip('"')
      tbl_data[ks][tbl] = {'type':'table', 'cql':line}
      tbl_data[ks][tbl]['field'] = {}
    elif("PRIMARY KEY" in line):
      if(line.count('(') == 1):
        tbl_data[ks][tbl]['pk'] = [line.split('(')[1].split(')')[0].split(', ')[0]]
        tbl_data[ks][tbl]['cc'] = line.split('(')[1].split(')')[0].split(', ')
        del tbl_data[ks][tbl]['cc'][0]
      elif(line.count('(') == 2):
        tbl_data[ks][tbl]['pk'] = line.split('(')[2].split(')')[0].split(', ')
        tbl_data[ks][tbl]['cc'] = line.split('(')[2].split(')')[1].lstrip(', ').split(', ')
      tbl_data[ks][tbl]['cql'] += ' ' + line.strip()
    elif line != '' and line.strip() != ');':
      try:
        tbl_data[ks][tbl]['cql'] += ' ' + line
        if('AND ' not in line and ' WITH ' not in line):
          fld_name = line.split()[0]
          fld_type = line.split()[1].strip(',')
          tbl_data[ks][tbl]['field'][fld_name]=fld_type
      except:
        print("Error1:" + ks + "." + tbl + " - " + line)

  for ks,readtable in read_table.items():
    for tablename,tablecount in readtable.items():
      read_count.append({'keyspace':ks,'table':tablename,'count':tablecount})

  for ks,writetable in write_table.items():
    for tablename,tablecount in writetable.items():
      write_count.append({'keyspace':ks,'table':tablename,'count':tablecount})

  read_count.sort(reverse=True,key=sortFunc)
  write_count.sort(reverse=True,key=sortFunc)

  #Create Cluster GC Spreadsheet
  workbook = xlsxwriter.Workbook(cluster_url + "/" + cluster_name + "_" + "load_data" + '.xlsx')
  worksheet = workbook.add_worksheet('RW Data')

  header_format1 = workbook.add_format({
      'bold': True,
      'italic' : True,
      'text_wrap': False,
      'font_size': 14,
      'border': 1,
      'valign': 'top'})

  header_format2 = workbook.add_format({
      'bold': True,
      'text_wrap': False,
      'font_size': 12,
      'border': 1,
      'valign': 'top'})

  data_format = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'valign': 'top'})

  row=0
  column=0
  for header in headers:
      worksheet.write(row,column,header,header_format1)
      column+=1
  
  perc_reads = 0.0
  row = 1
  column = 0
  for reads in read_count:
    perc_reads = float(read_subtotal) / float(total_reads)
    if (perc_reads <= read_threshold):
      ks = reads['keyspace']
      tbl = reads['table']
      cnt = reads['count']
      try:
        type(table_totals[ks])
      except:
        table_totals[ks] = {}
      table_totals[ks][tbl] = {'reads':cnt,'writes':'n/a'}
      read_subtotal += cnt
      worksheet.write(row,column,ks,data_format)
      worksheet.write(row,column+1,tbl,data_format)
      worksheet.write(row,column+2,cnt,data_format)
      worksheet.write(row,column+3,round(float(cnt)/total_reads*100,3),data_format)
      row+=1

  perc_writes = 0.0
  row = 1
  column = 5
  for writes in write_count:
    perc_writes = float(write_subtotal) / float(total_writes)
    if (perc_writes <= write_threshold):
      ks = writes['keyspace']
      tbl = writes['table']
      cnt = writes['count']
      try:
        type(table_totals[ks])
      except:
        table_totals[ks] = {}
      try:
        type(table_totals[ks][tbl])
        table_totals[ks][tbl] = {'reads':table_totals[ks][tbl]['reads'],'writes':cnt}
      except:
        table_totals[ks][tbl] = {'reads':'n/a','writes':cnt}
      write_subtotal += writes['count']
      worksheet.write(row,column,ks,data_format)
      worksheet.write(row,column+1,tbl,data_format)
      worksheet.write(row,column+2,cnt,data_format)
      worksheet.write(row,column+3,round(float(cnt)/total_writes*100,3),data_format)
      row+=1

  row = 1
  column = 10
  subtotal_count = write_subtotal+read_subtotal;
  for ks,ks_info in table_totals.items():
    for tbl,tbl_info in ks_info.items():
      worksheet.write(row,column,ks,data_format)
      worksheet.write(row,column+1,tbl,data_format)
      tbl_data[ks][tbl]['ratio']={}
      if (isinstance(tbl_info['reads'],int)):
        tbl_data[ks][tbl]['ratio']['read']=round(float(tbl_info['reads'])/subtotal_count*100,3)
        worksheet.write(row,column+2,tbl_data[ks][tbl]['ratio']['read'],data_format)
      else:
        worksheet.write(row,column+2,tbl_info['reads'],data_format)
      if (isinstance(tbl_info['writes'],int)):
        tbl_data[ks][tbl]['ratio']['write']=round(float(tbl_info['writes'])/subtotal_count*100,3)
        worksheet.write(row,column+3,tbl_data[ks][tbl]['ratio']['write'],data_format)
      else:
        worksheet.write(row,column+3,tbl_info['writes'],data_format)
      row+=1

  worksheet2 = workbook.add_worksheet('Field Data')

  row=0
  column=0
  for header in headers2:
      worksheet2.write(row,column,header,header_format1)
      column+=1
  
  row = 1
  column = 0
  for ks,ks_info in table_totals.items():
    for tbl,tbl_info in ks_info.items():
      try:
        for field_name,field_type in tbl_data[ks][tbl]['field'].items():
          worksheet2.write(row,column,ks,data_format)
          worksheet2.write(row,column+1,tbl,data_format)
          worksheet2.write(row,column+2,field_name,data_format)
          worksheet2.write(row,column+3,field_type,data_format)
          if (field_name in tbl_data[ks][tbl]['pk']):
            worksheet2.write(row,column+4,'x',data_format)
          if (field_name in tbl_data[ks][tbl]['cc']):
            worksheet2.write(row,column+5,'x',data_format)
          row+=1
      except:
        print("Error2:"+ks+"."+tbl)
        error=1
        
  worksheet3 = workbook.add_worksheet('Data Lists')

  workbook.close()

  # CREATE NOSQLBENCH YAML FILES
  if (include_yaml==1):

    # CREATE SCHEMA YAML
    schema_yaml = cluster_url + "/" + cluster_name + "_" + "schema.yaml"
    schemaFile = open(schema_yaml, "w")

    schemaFile.writelines(['description: Create Cassandra cluster ' + cluster_name + '\n\n','blocks:\n\n'])
    for ks,ks_info in table_totals.items():
      try:
        cql_array = tbl_data[ks]['cql'].split()
        cql_array.insert(2,'if not exists')
        cql = ' '.join(map(str, cql_array))
        if (new_dc!=''):
          cql_new=cql.split('{')[0]
          cql_new+=' { '+cql.split('{')[1].split(',')[0]+', '
          cql_new+=new_dc
          cql_new+=' } '+cql.split('{')[1].split('}')[1]
        else :
          cql_new=cql
        schemaTag(schemaFile,'create','keyspace',ks,tbl,cql_new)
        schemaTag(schemaFile,'drop','keyspace',ks,tbl,'DROP KEYSPACE if exists '+ks+';')
      except:
        print("Error3:"+ks)
      for tbl,tbl_info in ks_info.items():
        try:
          cql_array = tbl_data[ks][tbl]['cql'].split()
          cql_array.insert(2,'if not exists')
          cql = ' '.join(map(str, cql_array))
          schemaTag(schemaFile,'create','table',ks,tbl,cql)
          schemaTag(schemaFile,'drop','table',ks,tbl,'DROP TABLE if exists '+ks+'.'+tbl+';')
        except:
          print("Error4:"+ks+"."+tbl)

    schemaFile.close()

    # CREATE INITIAL LOAD YAML
    initial_load_yaml = cluster_url + "/" + cluster_name + "_" + "initial_load.yaml"
    initialLoadFile = open(initial_load_yaml, "w")

    initialLoadFile.writelines(['description: Initial load cluster ' + cluster_name + '\n\n','bindings:\n'])
    for ks,ks_info in table_totals.items():
      for tbl,tbl_info in ks_info.items():
        if (isinstance(tbl_info['reads'],int)):
          for fld_name,fld_type in tbl_data[ks][tbl]['field'].items():
            initialLoadFile.writelines(['  '+ks+'_'+tbl+'_'+fld_name+':\n'])

    initialLoadFile.write('\nblocks:\n\n',)
    for ks,ks_info in table_totals.items():
      for tbl,tbl_info in ks_info.items():
        if (isinstance(tbl_info['reads'],int)):
          rwTag(initialLoadFile,'write',ks,tbl,tbl_data[ks][tbl])

    # CREATE LOAD YAML
    load_yaml = cluster_url + "/" + cluster_name + "_" + "load.yaml"
    LoadFile = open(load_yaml, "w")

    LoadFile.writelines(['description: Load cluster ' + cluster_name + '\n\n','bindings:\n'])
    for ks,ks_info in table_totals.items():
      for tbl,tbl_info in ks_info.items():
        for fld_name,fld_type in tbl_data[ks][tbl]['field'].items():
          LoadFile.writelines(['  '+ks+'_'+tbl+'_'+fld_name+':\n'])

    LoadFile.write('\nblocks:\n\n',)
    for ks,ks_info in table_totals.items():
      for tbl,tbl_info in ks_info.items():
        if (isinstance(tbl_info['reads'],int)):
          rwTag(LoadFile,'read',ks,tbl,tbl_data[ks][tbl],'y')
        if (isinstance(tbl_info['writes'],int)):
          rwTag(LoadFile,'write',ks,tbl,tbl_data[ks][tbl],'y')

exit();

