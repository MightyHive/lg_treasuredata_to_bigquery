import pytd
import json
import os


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


def getTablesFromCurrentDatabase(x_client):
	tables = []
	results = x_client.query(f'SHOW TABLES')
	for result in results['data']:
		tables.append(result[0])
	return tables

def getBQSchemaFromTable(x_ds,x_client,table):
	
	c_name = ''
	c_type = ''
	
	columns = x_client.query( f'SHOW COLUMNS FROM {table}' )
	table_schema = []
	for column_td in columns['data']:
		column_bg = {}
  
		c_name = column_td[0]
		c_type = column_td[1]
  
		# datbase, table, column, data_type
		f_tmp.write(f'{x_ds},{table},{c_name},{c_type}\n')
  
		column_bg['name'] = c_name
		column_bg['type'] = getBQTypeFromTDType(c_type)
		table_schema.append(column_bg)
  
	return table_schema

def getBQTypeFromTDType(x_type):
 
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

def createJSONFile(x_ds, table, schema):
	with open(f"output/{x_ds}/{table}.json", "w") as outfile:
		json.dump(schema, outfile)
		print(f"{table}.json file created.")


def check_and_make_dir( x_ds_name ):
	path = 'output/' + x_ds_name

	# Check whether the specified path exists or not
	is_exist = os.path.exists(path)
 
	if not is_exist:
		# Create a new disrectory because it does not exist 
		os.makedirs(path)
		print(f'new dir created: {path}')
  
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
		tables = getTablesFromCurrentDatabase(client)
		for table in tables:

			table_schema = getBQSchemaFromTable(this_ds,client,table)
   
			createJSONFile(this_ds,table, table_schema)

	f_tmp.close()



if __name__ == "__main__":
	event = {}
	main(event)
	
