
# Treasure Data to BigQuery 

## Introduction

This is a one-time effort to migrate Treasure Data DB data to BigQuery. 

Challenges:
- size of data
- can ONLY run big queries after 20:00 KST

## BigQuery JSON Schema Mapping. 

The first big challenge is to create BQ JSON Schemas for each TD table

Some TD datatypes are easy enough to migrate:

- double --> FLOAT
- long --> INTEGER
- string --> STRING
- varchar --> STRING
- bigint --> NUMERIC
- datetime --> DATETIME
- date --> DATE

PROBLEM CASE
- TD table columns are sometimes NOT mapped to correct datatype
- In this case, we need to "manually" map it to the correct BQ Data 
- Example:  In TD, a "start_date" field is mapped to "varchar". It should be a "date".
- 2 different TD tables may have same field name, but 2 different data types. 
- This will take some manual effort. 


ACTION:
- make a "manual" mapping file:
-- TD_TABLE,TD_COLUMN,BQ_DATA_TYPE
-- adobe_general_b2b_by_evar,page_views,FLOAT





__END__