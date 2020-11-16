from schema_conversion import SchemaConversion
from database_config import ConvInitOption, ConvOutputOption
from data_conversion import DataConversion

if __name__ == '__main__':
	mysql_host = 'localhost'
	mysql_username = 'dangsg'
	mysql_password = 'Db@12345678'
	mysql_port = '3306'
	# mysql_dbname = 'sakila'
	mysql_dbname = 'employees'
	# info_level = 'standard'
	# schemacrawler_command = 'serialize'
	schema_conv_init_option = ConvInitOption(host = mysql_host, username = mysql_username, password = mysql_password, port = mysql_port, dbname = mysql_dbname)

	mongodb_host = 'localhost'
	mongodb_username = ''
	mongodb_password = ''
	mongodb_port = '27017'
	# mongodb_dbname = 'sakila'
	mongodb_dbname = 'employees'
	schema_conv_output_option = ConvOutputOption(host = mongodb_host, username = mongodb_username, password = mongodb_password, port = mongodb_port, dbname = mongodb_dbname)

	schema_conversion = SchemaConversion()
	schema_conversion.set_config(schema_conv_init_option, schema_conv_output_option)
	schema_conversion.run()
	# schema = schema_conversion.get()
	

	mysql2mongodb = DataConversion()
	mysql2mongodb.set_config(schema_conv_init_option, schema_conv_output_option, schema_conversion)
	mysql2mongodb.run()

	mysql2mongodb.validate()
	# mysql2mongodb.evaluate_validating()

	print("Done!")