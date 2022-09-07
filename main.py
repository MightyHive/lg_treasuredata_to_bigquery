import pytd
import json

def getTablesFromCurrentDatabase():
    tables = []
    results = client.query(f'SHOW TABLES')
    for result in results['data']:
        tables.append(result[0])
    return tables

def getBQSchemaFromTable(table):
    columns = client.query(
        f'SHOW COLUMNS FROM {table}')
    table_schema = []
    for column_td in columns['data']:
        column_bg = {}
        column_bg['name'] = column_td[0]
        column_bg['type'] = getBQTypeFromTDType(column_td[1])
        table_schema.append(column_bg)
    return table_schema

def getBQTypeFromTDType(type):
    match type:
        case 'double':
            return 'FLOAT'
        case 'long':
            return 'INTEGER'
        case 'string':
            return 'STRING'
    return type

def createJSONFile(table, schema):
    with open(f"output/{table}.json", "w") as outfile:
        json.dump(schema, outfile)
        print(f"{table}.json file created.")

# Create Client with TD_API_KEY and TD_API_SERVER define in variable envronment
# or use this client = pytd.Client(apikey='YOUR_API_KEY', endpoint='https://api.treasuredata.com/', database='database_name', default_engine='presto') 
client = pytd.Client(database='sample_datasets')

# Main
tables = getTablesFromCurrentDatabase()
for table in tables:
    table_schema = getBQSchemaFromTable(table)
    createJSONFile(table, table_schema)


