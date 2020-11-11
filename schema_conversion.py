import json, os
from utilities import extract_dict, import_json_to_mongodb, open_connection_mongodb
	
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
		with open(f"./intermediate_data/{self.schema_conv_init_option.dbname}/{self.schema_filename}") as schema_file:
			db_schema = json.load(schema_file)
		self.db_schema = db_schema
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
		# extracted_tables_schema = extract_tables_schema(tables_schema)
		table_name_list = list(map(lambda table: table["name"], self.extracted_tables_schema))
		return table_name_list

	def get_table_column_and_data_type(self):
		table_dict = self.get_tables_dict()
		all_columns = self.db_schema["all-table-columns"]
		schema_type_dict = {}
		for col in all_columns:
			dtype = col["column-data-type"]
			if type(dtype) is dict:
				schema_type_dict[dtype["@uuid"]] = dtype["name"].split()[0]
		table_list = self.get_tables_name_list()
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