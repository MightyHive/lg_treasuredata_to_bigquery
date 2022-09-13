import pytd
import json
import os
from datetime import datetime


# THIS_DS = '102_preprocessed_daily'
# THIS_DS = '102_preprocessed_weekly'
# THIS_DS = '102_preprocessed_monthly'
# THIS_DS = '102_preprocessed'
# THIS_DS = '101_raw'

DS_LIST  = [
	'102_preprocessed_daily',
 	'102_preprocessed_weekly',
	'102_preprocessed_monthly',
	'102_preprocessed',
	'101_raw',
]

# MANUALLY map TD table field name to BQ Data Type
# why?  many fields in TD table has wrong data type
# example - TD has many date fields as String, need to be date
# TD_TABLE,TD_TABLE_FIELD,BQ_DATA_TYPE
mapping_file = 'td_to_bq_field_mapping.csv'

# output ALL fields for comparison later
tmp_file = 'out.csv'

f_tmp = open(tmp_file, "w")

# Get all tables name from database
def get_tables_from_current_database(x_client):
	tables = []
	results = x_client.query(f'SHOW TABLES')
	for result in results['data']:
		tables.append(result[0])
	return tables

# Get BigQuery schema from table
def get_schema_from_table(x_ds, x_client, table, sample_row):
	
	c_name = ''
	c_type = ''
	
	columns = x_client.query( f'SHOW COLUMNS FROM {table}' )
	table_schema = []
	for index, column_td in enumerate(columns['data']):
		column_bg = {}

		c_name = column_td[0]
		c_type = column_td[1]

		# datbase, table, column, data_type
		f_tmp.write(f'{x_ds},{table},{c_name},{c_type}\n')

		column_bg['name'] = c_name
		column_bg['type'] = get_BQ_type_from_TD_type(c_type)
		column_bg['sample_value'] = sample_row[index]
		table_schema.append(column_bg)
  
	return table_schema

# Convert types
def get_BQ_type_from_TD_type(x_type):
 
	if x_type == 'double':
		return 'FLOAT'
	elif x_type == 'long':
		return 'INTEGER'
	elif x_type == 'string':
		return 'STRING'
	elif x_type == 'varchar':
		return 'STRING'
	elif x_type == 'bigint':
		return 'NUMERIC'
	elif x_type == 'datetime':
		return 'DATETIME'
	elif x_type == 'date':
		return 'DATE'

	return x_type

# Create json file with table schema
def create_json_file(x_ds, table, schema):
	with open(f"output/{x_ds}/{table}.json", "w") as outfile:
		json.dump(schema, outfile)
		print(f"{table}.json file created.")

# Check and create directory
def check_and_make_dir( x_ds_name ):
	path = 'output/' + x_ds_name

	# Check whether the specified path exists or not
	is_exist = os.path.exists(path)
 
	if not is_exist:
		# Create a new disrectory because it does not exist 
		os.makedirs(path)
		print(f'new dir created: {path}')

# Get a sample row to define if there's date column
def get_sample_row_from_table(client, table):
    results = client.query(f'SELECT * FROM {table} LIMIT 1')
    return results

# Update schema with date column
def update_schema_column_date_type(table_schema):
	formats = ['%Y-%m-%d', '%Y-%m', '%Y-%m-%d %H:%M:%S']
	for column in table_schema:
		for format in formats:
			try:
				if bool(datetime.strptime(str(column['sample_value']), format)):
					column['type'] = 'DATE'
			except ValueError:
				error = ValueError

# Remove the sample value from schema
def remove_sample_value_from_schema(table_schema):
	for column in table_schema:
		column.pop('sample_value')

### end functions



# main section

def main(event):
	for this_ds in DS_LIST:
		check_and_make_dir(this_ds)
		# Create Client with TD_API_KEY and TD_API_SERVER define in variable envronment
		# or use this client = pytd.Client(apikey='YOUR_API_KEY', endpoint='https://api.treasuredata.com/', database='database_name', default_engine='presto') 
		# client = pytd.Client( apikey='dsafasfdsf',endpoint='https://api.treasuredata.com/', database='sample_dataset_2')
		client = pytd.Client( endpoint='https://api.treasuredata.com/', database=this_ds)

		# Main
		tables = get_tables_from_current_database(client)
		for table in tables:
			sample_row = get_sample_row_from_table(client, table)
			if sample_row['data']:
				table_schema = get_schema_from_table(this_ds,client,table, sample_row['data'][0])
				update_schema_column_date_type(table_schema)
				remove_sample_value_from_schema(table_schema)
				create_json_file(this_ds,table, table_schema)
			else:
				print(f'No data for this table : {table}')
			
	f_tmp.close()

if __name__ == "__main__":
	event = {}
	main(event)
	
