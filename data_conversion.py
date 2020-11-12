import sys, json, bson
from schema_conversion import MySQLDatabaseSchema
from utilities import open_connection_mysql, open_connection_mongodb, import_json_to_mongodb, extract_dict, store_json_to_mongodb
from bson.decimal128 import Decimal128
import decimal
from bson import BSON
	
class MySQL2MongoDB:
	"""docstring for DatabaseData"""
	def __init__(self, schema_conv_init_option, schema_conv_output_option, schema):
		super(MySQL2MongoDB, self).__init__()
		self.schema = schema
		#set config
		self.schema_conv_init_option = schema_conv_init_option
		self.schema_conv_output_option = schema_conv_output_option
		#migrate from mysql to json
		#migrate from json to mongodb
		#read schema

		#convert relation
		#convert datatype 
		#convert...

	def find_target_dtype(self, dtype, dtype_dict, mongodb_dtype):
		for target_dtype in dtype_dict.keys():
			if(dtype) in dtype_dict[target_dtype]:
				return mongodb_dtype[target_dtype]
		return None 

	def migrate_mysql_to_mongodb(self):
		#get connection
		connection_info = {}
		connection_info["host"] = self.schema_conv_init_option.host #"localhost"
		connection_info["username"] = self.schema_conv_init_option.username #"dangsg"
		connection_info["password"] = self.schema_conv_init_option.password #"Db@12345678"
		connection_info["database"] = self.schema_conv_init_option.dbname #"sakila"

		mongodb_dtype = {
			"integer": "integer",
			"decimal": "decimal",
			"double": "double",
			"boolean": "boolean",
			"date": "date",
			"timestamp": "timestamp",
			"binary": "binary",
			"blob": "blob",
			"string": "string",
			"object": "object",
			"single-geometry": "single-geometry",
			"multiple-geometry": "multiple-geometry",
		}
		
		dtype_dict = {}
		dtype_dict[mongodb_dtype["integer"]] = ["TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT"]
		dtype_dict[mongodb_dtype["decimal"]] = ["DECIMAL", "DEC", "FIXED"]
		dtype_dict[mongodb_dtype["double"]] = ["FLOAT", "DOUBLE", "REAL"]
		dtype_dict[mongodb_dtype["boolean"]] = ["BOOL", "BOOLEAN"]
		dtype_dict[mongodb_dtype["date"]] = ["DATE", "YEAR"]
		dtype_dict[mongodb_dtype["timestamp"]] = ["DATETIME", "TIMESTAMP", "TIME"]
		dtype_dict[mongodb_dtype["binary"]] = ["BIT", "BINARY", "VARBINARY"]
		dtype_dict[mongodb_dtype["blob"]] = ["TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"]
		dtype_dict[mongodb_dtype["string"]] = ["CHARACTER", "CHARSET", "ASCII", "UNICODE", "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"]
		dtype_dict[mongodb_dtype["object"]] = ["ENUM", "SET"]
		dtype_dict[mongodb_dtype["single-geometry"]] = ["GEOMETRY", "POINT", "LINESTRING", "POLYGON"]
		dtype_dict[mongodb_dtype["multiple-geometry"]] = ["MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"]

		auto_convert_set = set(set(mongodb_dtype.values()))
		manual_convert_set = set(mongodb_dtype.values()) - auto_convert_set
		# print(auto_convert_set)
		# print(manual_convert_set)

		try:
			db_connection = open_connection_mysql(connection_info)
			if db_connection.is_connected():
				#start migrating
				#read table_column_dtype_dict
				table_column_dtype_dict = self.schema.get_table_column_and_data_type()
				# print(table_column_dtype_dict)
				# return
				#for table in table_column_dtype_dict
				for table in table_column_dtype_dict.keys():
					col_fetch_seq = []
					sql_cmd = "SELECT"
					#for col in table
					for col in table_column_dtype_dict[table].keys():
						col_fetch_seq.append(col)
						dtype = table_column_dtype_dict[table][col]
						target_dtype = self.find_target_dtype(dtype, dtype_dict, mongodb_dtype)
						#generate SQL
						if target_dtype is None:
							raise Exception(f"Data type {dtype} has not been handled!")
						elif target_dtype == mongodb_dtype["single-geometry"]:
							sql_cmd = sql_cmd + " AsText(" + col + "),"
						else:
							sql_cmd = sql_cmd + " `" + col + "`,"
					#join sql
					sql_cmd = sql_cmd[:-1] + " FROM " + table
					# print(sql_cmd)
					db_cursor = db_connection.cursor();
					#execute sql
					db_cursor.execute(sql_cmd)
					#fetch data and convert ###NOT CONVERT YET
					fetched_data = db_cursor.fetchall()
					# print(col_fetch_seq)
					# print(fetched_data[0])
					rows = []
					for row in fetched_data:
						data = {}
						for i in range(len(col_fetch_seq)):
							col = col_fetch_seq[i]
							dtype = table_column_dtype_dict[table][col]
							target_dtype = self.find_target_dtype(dtype, dtype_dict, mongodb_dtype)
							#generate SQL
							if dtype == "GEOMETRY":
								geodata = row[i][6:-1].split()
								# print(row[i][0])
								# print(geodata)
								converted_data = {
									"type": "Point",
									"coordinates": geodata[:]
								}
							elif target_dtype == mongodb_dtype["decimal"]:
								converted_data = Decimal128(row[i])
							elif target_dtype == mongodb_dtype["object"]:
								if type(row[i]) is str:
									converted_data = [row[i]]
								else:
									converted_data = tuple(row[i])
								# converted_data = row[i]
								# print(row[i], converted_data)
								# print(type(row[i]), type(converted_data))
							elif dtype == "VARCHAR":
								# print(row[i])
								# print(type(row[i]))
								converted_data = str(row[i])
								# if col == "password":
								# print(converted_data)
								# print(type(converted_data))
							else:
								converted_data = row[i]
							data[col_fetch_seq[i]] = converted_data 
						# print(data)
						rows.append(data)
					db_cursor.close()
					#assign to obj
					#store to mongodb
					# print("Start migrating table ", table)
					mongodb_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname)
					store_json_to_mongodb(mongodb_connection, table, rows)
				print("Migrate data from MySQL to MongoDB file successfully!")
			else:
				print("Connect fail!")
		
		except Exception as e:
			print("Error while writing to MongoDB", e)
		
		finally:
			if (db_connection.is_connected()):
				db_connection.close()
				print("MySQL connection is closed!")


	# def migrate_mysql_to_mongodb(self):
	# 	#get connection
	# 	connection_info = {}
	# 	connection_info["host"] = self.schema_conv_init_option.host #"localhost"
	# 	connection_info["username"] = self.schema_conv_init_option.username #"dangsg"
	# 	connection_info["password"] = self.schema_conv_init_option.password #"Db@12345678"
	# 	connection_info["database"] = self.schema_conv_init_option.dbname #"sakila"
		
	# 	try:
	# 		db_connection = open_connection_mysql(connection_info)
	# 		if db_connection.is_connected():
	# 			#start migrating
	# 			tables_name_list = self.schema.get_tables_name_list()
	# 			for table_name in tables_name_list:
	# 				rows = self.fetch_table_rows(db_connection, table_name)
	# 				columns = self.fetch_table_columns(db_connection, table_name)
	# 				json_data = self.convert_fetched_data_to_json(table_name, rows, columns)
	# 				mongodb_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname)
	# 				store_json_to_mongodb(mongodb_connection, json_data["table"], json_data["data"])
					
	# 			print("Migrate data from MySQL to MongoDB file successfully!")
	# 		else:
	# 			print("Connect fail!")
		
	# 	except Exception as e:
	# 		print("Error while writing to MongoDB", e)
		
	# 	finally:
	# 		if (db_connection.is_connected()):
	# 			db_connection.close()
	# 			print("MySQL connection is closed!")

	def migrate_json_to_mongodb(self):
		db_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname)
		tables_and_views_name_list = self.schema.get_tables_and_views_list()
		for table_name in tables_and_views_name_list:
			collection_name = table_name
			json_filename = collection_name + ".json"
			import_json_to_mongodb(db_connection, collection_name, self.schema_conv_output_option.dbname, json_filename, True)
		print("Migrate data from JSON to MongoDB successfully!")

	def convert_relations_to_references(self):
		tables_name_list = self.schema.get_tables_name_list()
		# db_connection = open_connection_mongodb(mongodb_connection_info)
		tables_relations = self.schema.get_tables_relations()
		# converting_tables_order = specify_sequence_of_migrating_tables(schema_file)
		edited_table_relations_dict = {}
		original_tables_set = set([tables_relations[key]["source-table"] for key in tables_relations])

		# Edit relations of table dictionary
		for original_table in original_tables_set:
			for key in tables_relations:
				if tables_relations[key]["source-table"] == original_table:
					if original_table not in edited_table_relations_dict.keys():
						edited_table_relations_dict[original_table] = []
					edited_table_relations_dict[original_table] = edited_table_relations_dict[original_table] + [extract_dict(["source-column", "dest-table", "dest-column"])(tables_relations[key])]
		# print(tables_relations)
		# Convert each relation of each table
		# for order in range(int(max(converting_tables_order.keys())) + 1):
		for original_collection_name in tables_name_list:
			# for original_collection_name in converting_tables_order[str(order)]:
			if original_collection_name in original_tables_set:
				for relation_detail in edited_table_relations_dict[original_collection_name]:
					referencing_collection_name = relation_detail["dest-table"]
					original_key = relation_detail["source-column"]
					referencing_key = relation_detail["dest-column"]
					# print(original_collection_name, referencing_collection_name, original_key, referencing_key)
					self.convert_one_relation_to_reference(original_collection_name, referencing_collection_name, original_key, referencing_key) 
		print("Convert relations successfully!")


	def convert_one_relation_to_reference(self, original_collection_name, referencing_collection_name, original_key, referencing_key):
		db_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname)
		original_collection_connection = db_connection[original_collection_name]
		# print(original_collection_name)
		original_documents = original_collection_connection.find()
		new_referenced_key_dict = {}
		for doc in original_documents:
			# print(doc)
			new_referenced_key_dict[doc[original_key]] = doc["_id"]
		# print(original_documents)
		# print(new_referenced_key_dict)

		referencing_documents = db_connection[referencing_collection_name]
		for key in new_referenced_key_dict:
			new_reference = {}
			new_reference["$ref"] = original_collection_name
			new_reference["$id"] = new_referenced_key_dict[key]
			new_reference["$db"] = self.schema_conv_output_option.dbname
			referencing_documents.update_many({referencing_key: key}, update={"$set": {referencing_key: new_reference}})


	# def convert_one_relation_to_reference(self, original_collection_name, referencing_collection_name, original_key, referencing_key):
	# 	db_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname)
	# 	original_collection_connection = db_connection[original_collection_name]
	# 	# print(original_collection_name)
	# 	original_documents = original_collection_connection.find()
	# 	new_referenced_key_dict = {}
	# 	for doc in original_documents:
	# 		# print(doc)
	# 		new_referenced_key_dict[doc[original_key]] = doc["_id"]
	# 	# print(original_documents)
	# 	# print(new_referenced_key_dict)

	# 	referencing_documents = db_connection[referencing_collection_name]
	# 	for key in new_referenced_key_dict:
	# 		new_reference = {}
	# 		new_reference["$ref"] = original_collection_name
	# 		new_reference["$id"] = new_referenced_key_dict[key]
	# 		referencing_documents.update_many({referencing_key: key}, update={"$set": {referencing_key: new_reference}})


	def fetch_table_rows(self, db_connection, table_name):
		"""Fetch all rows of specific table"""
		db_cursor = db_connection.cursor();
		db_cursor.execute("SELECT * FROM " + table_name)
		fetched_data = db_cursor.fetchall()
		rows = []
		for row in fetched_data:
			rows.append(row)
		db_cursor.close()
		return rows

	def fetch_table_columns(self, db_connection, table_name):
		"""Fetch columns/attribute of table"""
		db_cursor = db_connection.cursor()
		db_cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table_name + "'")
		fetched_data = db_cursor.fetchall()
		columns = []
		for column in fetched_data:
			columns.append(column[0])
		db_cursor.close()
		return columns

	def convert_fetched_data_to_json(self, table_name, rows, columns):
		"""Convert data from table to json"""
		dataset = []
		# print(table_name)
		for row in rows:
			row_dict = {}
			for i in range(len(columns)):
				# row_dict[columns[i]] = str(row[i])
				data = self.convert_to_bson(row[i], columns[i])
				# if(type(data) is bytearray):
				# 	data = str(data)
				# elif(type(data) is decimal.Decimal):
				# 	data = Decimal128(data)
				# elif(type(data) is set):
				# 	data = list(data)
				# print(row[i])
				row_dict[columns[i]] = data
				# print(type(row[i]))
			dataset.append(row_dict)
		json_data = {}
		json_data["table"] = table_name
		json_data["data"] = dataset
		# for dat in dataset[0].values():
			# print(dat, type(dat))
		return json_data

	def convert_to_bson(self, data, col):
		if type(data) is bytearray:
			print(data, col)
		return data

	# def write_json_to_file(self, json_data, filename):
	# 	"""Write json data to file"""
	# 	with open(f"./intermediate_data/{self.schema_conv_init_option.dbname}/{filename}", 'w') as outfile:
	# 		json.dump(json_data, outfile, default=str)

