import json, os
from utilities import extract_dict, import_json_to_mongodb, open_connection_mongodb
from collections import OrderedDict
	
class MySQLDatabaseSchema:
	"""docstring for DatabaseSchema"""
	def __init__(self, schema_conv_init_option, schema_conv_output_option):
		super(MySQLDatabaseSchema, self).__init__()
		self.schema_filename = "schema.json"
		#set config
		self.schema_conv_init_option = schema_conv_init_option
		self.schema_conv_output_option = schema_conv_output_option
		#generate schema to file
		#load schema from file into data object

	def load_schema(self):
		if not hasattr(self, "db_schema"):
			with open(f"./intermediate_data/{self.schema_conv_init_option.dbname}/{self.schema_filename}") as schema_file:
				db_schema = json.load(schema_file)
			self.db_schema = db_schema
			self.all_table_columns = self.db_schema["all-table-columns"]
			self.tables_schema = self.db_schema["catalog"]["tables"]
			self.extracted_tables_schema = self.extract_tables_schema()
	
	def generate_mysql_schema(self, info_level="maximum"):
		command_create_intermediate_dir = f"mkdir -p ./intermediate_data/{self.schema_conv_init_option.dbname}"
		os.system(command_create_intermediate_dir)
		command = f"schemacrawler.sh \
		--server=mysql \
		--host={self.schema_conv_init_option.host} \
		--port={self.schema_conv_init_option.port} \
		--database={self.schema_conv_init_option.dbname} \
		--schemas={self.schema_conv_init_option.dbname} \
		--user={self.schema_conv_init_option.username} \
		--password={self.schema_conv_init_option.password} \
		--info-level={info_level} \
		--command=serialize\
		--output-file=./intermediate_data/{self.schema_conv_init_option.dbname}/{self.schema_filename}"
		os.system(command)
		print(f"Generate MySQL database {self.schema_conv_init_option.dbname} successfully!")
		return True

	def save_schema(self):
		"""Save schema to MongoDB database"""
		db_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname) 
		# print("Ready to write!")
		import_json_to_mongodb(db_connection, collection_name="schema", dbname=self.schema_conv_output_option.dbname, json_filename=self.schema_filename)
		print(f"Save schema from {self.schema_conv_output_option.dbname} database to MongoDB successfully!")
		return True

	def extract_tables_schema(self, extracted_keys_list = ["@uuid", "name", "columns", "foreign-keys"]):
		"""Extract only specific fields from tables schema""" 
		ite_func = extract_dict(extracted_keys_list)
		return list(map(ite_func, self.tables_schema))	

	def get_columns_dict(self):
		"""
		Extract column uuid and name from database schema
		Return a dictionary with @uuid as key and column name as value
		"""
		all_table_columns = self.db_schema["all-table-columns"]
		col_dict = {}
		for col in all_table_columns:
			col_dict[col["@uuid"]] = col["name"]
		return col_dict

	def get_tables_dict(self):
		"""
		Extract column uuid and its table name from database schema
		Return a dictionary with @uuid as key and table name as value
		"""
		table_dict = {}
		for table in self.tables_schema:
			for col in table["columns"]:
				table_dict[str(col)] = table["name"]
		return table_dict


	def get_tables_relations(self):
		"""
		Get relations between MySQL tables from database schema.
		Result will be a dictionary which has uuids (of relation, defined by SchemaCrawler) as keys, and values including:
		- source: Name of table which holds primary key of relation
		- dest: Name of table which holds foreign key of relation
		"""
		col_dict = self.get_columns_dict()
		table_dict = self.get_tables_dict()

		relations_dict = {}
		for table in self.extracted_tables_schema:
			for foreign_key in table["foreign-keys"]:
				if(isinstance(foreign_key, dict)):
					relation_uuid = foreign_key["@uuid"]
					foreign_key_uuid = foreign_key["column-references"][0]["foreign-key-column"]
					primary_key_uuid = foreign_key["column-references"][0]["primary-key-column"]
					relations_dict[relation_uuid] = {}
					relations_dict[relation_uuid]["source-table"] = table_dict[primary_key_uuid]
					relations_dict[relation_uuid]["dest-table"] = table_dict[foreign_key_uuid]
					relations_dict[relation_uuid]["source-column"] = col_dict[primary_key_uuid]
					relations_dict[relation_uuid]["dest-column"] = col_dict[foreign_key_uuid]
		# for table in self.extracted_tables_schema:
			# for foreign_key in table["foreign-keys"]:
				# if(isinstance(foreign_key, str)):
					# relations_dict[str(foreign_key)]["source-table"] = table["name"]
					# pass
		# print(relations_dict)
		return relations_dict

	def get_tables_name_list(self):
		"""Get list of name of all tables from table schema"""
		self.load_schema()
		table_name_list = list(map(lambda table: table["name"], list(filter(lambda table: table["remarks"] == "", self.tables_schema))))
		return table_name_list

	def get_tables_and_views_list(self):
		table_and_view_name_list = list(map(lambda table: table["name"], self.extracted_tables_schema))
		return table_and_view_name_list

	def get_table_column_and_data_type(self):
		self.load_schema()
		table_dict = self.get_tables_dict()
		all_columns = self.db_schema["all-table-columns"]
		schema_type_dict = {}
		for col in all_columns:
			dtype = col["column-data-type"]
			if type(dtype) is dict:
				schema_type_dict[dtype["@uuid"]] = dtype["name"].split()[0]
		table_list = self.get_tables_and_views_list()
		res = {}
		for table_name in table_list:
			res[table_name] = {}
		for col in all_columns:
			dtype = col["column-data-type"]
			if type(dtype) is dict:
				res[table_dict[col["@uuid"]]][col["name"]] = schema_type_dict[dtype["@uuid"]]
			else:
				res[table_dict[col["@uuid"]]][col["name"]] = schema_type_dict[dtype]
		return res

	def convert_to_mongo_schema(self):
		table_view_column_dtype = self.get_table_column_and_data_type()
		table_list = self.get_tables_name_list()
		uuid_col_dict = self.get_columns_dict()
		table_column_dtype = {}
		for table in table_list:
			table_column_dtype[table] = table_view_column_dtype[table]
		table_cols_uuid = {}
		for table in self.tables_schema:
			table_name = table["name"]
			if table_name in table_list:
				table_cols_uuid[table_name] = table["columns"]
		enum_col_dict = {}
		for col in self.all_table_columns:
			if col["attributes"]["COLUMN_TYPE"][:4] == "enum":
				# print(col["short-name"])
				data = {}
				table_name, col_name = col["short-name"].split(".")[:2]
				if table_name in table_list:
					# data = {}
					# data[col_name] = col_name.
					# print(table_name, col_name)
					data = list(map(lambda ele: ele[1:-1], col["attributes"]["COLUMN_TYPE"][5:-1].split(",")))
					sub_dict = {}
					sub_dict[col_name] = data
					enum_col_dict[table_name] = sub_dict
		# for table in table_name_list:
		for table in table_cols_uuid:
			props = {}
			for col_uuid in table_cols_uuid[table]:
				col_name = uuid_col_dict[col_uuid]
				mysql_dtype = table_column_dtype[table][col_name]
				if mysql_dtype == "ENUM":
					data = {
						"enum": enum_col_dict[table][col_name],
						"description": "can only be one of the enum values"
					}
				else:
					data = {
						"bsonType": self.data_type_schema_mapping(mysql_dtype)
					}
				props[col_name] = data
				json_schema = {}
				json_schema["bsonType"] = "object"
				json_schema["properties"] = props
				db_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname) 
				db_connection.drop_collection(table)
				db_connection.create_collection(table)
				vexpr = {"$jsonSchema": json_schema}
				cmd = OrderedDict([('collMod', table), ('validator', vexpr)])
				db_connection.command(cmd)
		# for table in table_list:
		# 	#decl jsonSchema
		# 	props = {}
		# 	#for col in table
		# 	cols_in_table = table_view_column_dtype[table]
		# 	for col in cols_in_table:
		# 		#add col details to jsonSchema
		# 		mysql_dtype = cols_in_table[col]
		# 		data = {}
		# 		if(mysql_dtype == "ENUM"):
		# 			# data["enum"] =
		# 			pass 
		# 		else:
		# 		props[col] = data
		# 	#add jsonSchema to db
		# 	json_schema = {}
		# 	json_schema["bsonType"] = "object"
		# 	json_schema["properties"] = props
		# 	db_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname) 
		# 	db_connection.drop_collection(table)
		# 	db_connection.create_collection(table)
		# 	vexpr = {"$jsonSchema": json_schema}
		# 	cmd = OrderedDict([('collMod', table),
	 #        ('validator', vexpr)])
		# 	db_connection.command(cmd)

	def data_type_schema_mapping(self, mysql_type):
		dtype_dict = {}
		dtype_dict["int"] = ["TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER"] 
		dtype_dict["long"] = ["BIGINT"]
		dtype_dict["decimal"] = ["DECIMAL", "DEC", "FIXED"]
		dtype_dict["double"] = ["FLOAT", "DOUBLE", "REAL"]
		dtype_dict["bool"] = ["BOOL", "BOOLEAN"]
		dtype_dict["date"] = ["DATE", "YEAR", "DATETIME", "TIMESTAMP", "TIME"]
		# dtype_dict["timestamp"] = []
		dtype_dict["binData"] = ["BIT", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"]
		# dtype_dict["blob"] = []
		dtype_dict["string"] = ["CHARACTER", "CHARSET", "ASCII", "UNICODE", "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"]
		dtype_dict["object"] = ["ENUM", "SET", "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"]
		# dtype_dict["single-geometry"] = []
		# dtype_dict["multiple-geometry"] = []

		for mongodb_type in dtype_dict.keys():
			if mysql_type in dtype_dict[mongodb_type]:
				# print(mysql_type, mongodb_type)
				return mongodb_type
		print(f"MySQL data type {mysql_type} has not been handled!")
		return None