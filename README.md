# NB_Load_Extractor

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [Summary](#summary)
* [Getting Started](#getting-started)
* [Using the Cluster Load Spreadsheet](#using-the-cluster-load-spreadsheet)
* [NoSQLBench YAML Files](#nosqlbench-yaml-files)
* [Collecting Diagnostic Data](#collecting-diagnostic-data)
* [Acknowledgements](#acknowledgements)

<!-- SUMMARY -->
## Summary

This script was designed to supplement NoSQLBench by identifying primary application load tables with read/write ratios based on data from the DSE OpsCenter diagnostic tarball.  It produces a Excel spreadsheet with read and write traffic totals for application tables in the Cassandra cluster.  It also includes a list of fields identifying primary keys and field types.  There is also an option to create yaml files to be used by NoSQLBench.

This script is also very helpful with identifying the table traffic volumes.  For example, if a table with a large partition is rarely used, fixing it may not signifantly improve performance.  On the other hand, if a table with reads that total 55% of the cluster's total read/write transactions has a high number of SSTables, this may be significantly affecting overall cluster performance.

<!-- GETTING STARTED -->
## Getting Started

After cloning the NB Load Extractor project, download a diagnostic tarball from a targeted Cassandra cluster through DSE OpsCenter.

### Commands and Arguments

#### Creating the Cluster Load Spreadsheet
To create the 
the cluster load data spreadsheet run the following command:
```
python extract_load.py -p [path_to_diag_folder]
```
You may run the script on multiple diagnostic folders at a time:
```
python extract_load.py -p [path_to_diag_folder1] -p [path_to_diag_folder2] -p [path_to_diag_folder3]
```

#### Creating the NoSQLBench YAML Files
To include the NoSQLBench yaml files:
```
python extract_load.py -p [path_to_diag_folder] -inc_yaml
```

#### Modifying Read/Write Load included in Documents
Many times, a few tables handle most of the read and write traffic.  Replicating the load on these main tables usually requires significantly less time than replicating the entire load.  this is due to the increased number of binding statements required to create on the entire load. For example, it may take 200 binding statements to run an entire cluster, where only 30 are required to use on the main tables.  For this reason, the -rt read threshold and -wt write threshold arguments were created.  By limited the read threshold to 85%, the number of binding statements needed are usually significantly reduced without effecting the load testing (in most cases).  The default values of -rt and -wt are 85%.
Changing the -rt to 95% and the -wt to 98%:
```
python extract_load.py -p [path_to_diag_folder] -rt 95 -wt 98%
```

#### Help
There is a brief help info section:
```
python extract_load.py --help
```

## Using the Cluster Load Spreadsheet
There are three tabs of data in this spreadsheet.  

### *RW Data* Tab
* Columns A-D lists the table read information.
* Columns F-I lists the table write information. 
*Note: Remember this may not include all the tables which have reads.  The read and/or write threshold argument may be limiting the tables to only include the top 80% if -rt 80 -wt 80 was used in the command.*
* Columns K-N lists the table read and write percent of the total included (threshold data only) RW traffic.

### *Field Data* Tab
This tab would be provided to to the cluster data SME to provide information to build the binding statements.
* Column A: Keyspace
* Column B: Table
* Column C: Field Name
* Column D: Field Type (in the future this will be used to create a default binding statement)
* Column E: Signifies whether this field is a partition key
* Column F: Signifies whether this field is a clustering column
* Column G: Signifies whether the data in the field is limited to a limited number of values (i.e. a status field may be limited to five stages)
* Column H: Field Length
* Column I: Data Pattern
* Column J: Example Data
* Column K: Binding Statement

### *Data Lists* Tab
This tab would contain the list of values with fields where column G has been marked.  The field name will be in row 1 and the possible values in the following rows. To the right of the in the next column would be the percentage of times this value would be used. If it is equal percent across all values, this column may be left blank.  Each list will have two columns.

## NoSQLBench YAML Files
This application automatically creates three yaml files to be used in for NoSQLBench (NB).
[Learn more about the NoSQLBench project](https://www.datastax.com/blog/nosqlbench)
*NOTE: if using different DC(s) than are in the diagnostic files, make sure to include the correct DC name(s) in the new_dc variable* 

### *Schema* YAML
This script is used to create the cluster's schema.
Example NB command line to create keyspace, tables and indexes
```
nb run type=cql yaml=[cluster_name]_schema tags=phase:create_schema.* host=[Single Node IP] threads=1
```
```
nb run type=cql yaml=[cluster_name]_schema tags=phase:create_table.* host=[Single Node IP] threads=1
```
Example NB command line to drop keyspace
```
nb run type=cql yaml=[cluster_name]_schema tags=phase:drop_schema.* host=[Single Node IP] threads=1
```

### *Initial Load* YAML
This script is used to initially load the cluster's read tables.
Example NB command line
```
nb run type=cql yaml=[cluster_name]_initial_load tags=phase:write_[keyspace]_[table] host=[Single Node IP] cycles=[number of records]
```

### *Load* YAML
This script is used to simulate and actual load on the cluster based on the traffic in the diagnostic files.
Example NB command line
```
nb run type=cql yaml=[cluster_name]_load tags=phase:load.* host=[ALL Node IPs] cycles=[very large number i.e. 10B] cyclerate=[Controll TPS Here]
```

## Collecting Diagnostic Data

### Automated Diagnostic Collection through OpsCenter
Opscenter is the easiest way to collect a diagnostic tarbal. Download a compressed tarball that contains diagnostic information about the OpsCenter daemon and all the nodes in a specific cluster. [Instructions Here](https://docs.datastax.com/en/opscenter/6.7/opsc/online_help/opscCollectingDiagnosticData_t.html)

### Manual Diagnostic Collection
Collect the following from all nodes and place the outputs/files in a directory with the node's IP address as the directory name:
* nodetool cfstats > ./nodetool/cfstats
* nodetool describecluster > ./nodetool/describecluster
* cqlsh -e "describe full schema;" > ./driver/schema

## Acknowledgements
Special thanks to Shooky for leading the creation of NoSQLBench.  It is an insiring application that has been a tremendous tool for load testing a cluster and many other tasks. 
