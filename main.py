from schema_conversion import MySQLDatabaseSchema
from database_config import ConvInitOption, ConvOutputOption
from data_conversion import MySQL2MongoDB

if __name__ == '__main__':
	mysql_host = 'localhost'
	mysql_username = 'dangsg'
	mysql_password = 'Db@12345678'
	mysql_port = '3306'
	mysql_dbname = 'sakila'
	# info_level = 'standard'
	# schemacrawler_command = 'serialize'
	schema_conv_init_option = ConvInitOption(host = mysql_host, username = mysql_username, password = mysql_password, port = mysql_port, dbname = mysql_dbname)

	mongodb_host = 'localhost'
	mongodb_username = ''
	mongodb_password = ''
	mongodb_port = '27017'
	mongodb_dbname = 'sakila'
	schema_conv_output_option = ConvOutputOption(host = mongodb_host, username = mongodb_username, password = mongodb_password, port = mongodb_port, dbname = mongodb_dbname)

	mysql_database_schema = MySQLDatabaseSchema(schema_conv_init_option, schema_conv_output_option)
	mysql_database_schema.drop_mongodb()
	# mysql_database_schema.generate_mysql_schema()
	# mysql_database_schema.save_schema()
	# mysql_database_schema.load_schema()
	mysql_database_schema.convert_to_mongo_schema()
	mysql_database_schema.convert_index()

	mysql2mongodb = MySQL2MongoDB(schema_conv_init_option, schema_conv_output_option, mysql_database_schema)
	mysql2mongodb.migrate_mysql_to_mongodb()
	mysql2mongodb.convert_relations_to_references()

