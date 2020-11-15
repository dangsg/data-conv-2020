import sys, json, bson, re
from schema_conversion import SchemaConversion
from utilities import open_connection_mysql, open_connection_mongodb, import_json_to_mongodb, extract_dict, store_json_to_mongodb
from bson.decimal128 import Decimal128
from decimal import Decimal
from bson import BSON
	
class DataConversion:
	"""
	DataConversion Database data class.
	This class is used for:
		- Converting and migrating data from MySQL to MongoDB.
		- Validating if converting is correct, using re-converting method.
	"""
	def __init__(self):
		super(DataConversion, self).__init__()

	def set_config(self, schema_conv_init_option, schema_conv_output_option, schema):
		"""
		To set config, you need to provide:
			- schema_conv_init_option: instance of class ConvInitOption, which specified connection to "Input" database (MySQL).
			- schema_conv_output_option: instance of class ConvOutputOption, which specified connection to "Out" database (MongoDB).
			- schema: MySQL schema object which was loaded from MongoDB.
		"""
		self.schema = schema
		#set config
		self.schema_conv_init_option = schema_conv_init_option
		self.schema_conv_output_option = schema_conv_output_option
		self.validated_dbname = self.schema_conv_init_option.dbname + "_validated"

	def run(self):
		self.__save()
		self.validate()

	def __save(self):
		self.migrate_mysql_to_mongodb()
		self.convert_relations_to_references()

	def validate(self):
		"""
		Convert data from MongoDB back to MySQL and evaluate.
		"""
		# 1 Create Database
		# 2 Create Schema
		# 3 Define MySQL schema
			# 3.1 Define tables
			# 3.2 Define columns of each tables
			# 3.3 Deinfe constraint
		# 4 Import data to MySQL
		# 5 Evaluate 

		mysql_connection = self.create_validated_database()
		self.create_validated_tables(mysql_connection)

		if mysql_connection.is_connected():
			mysql_connection.close()
			print("Disconnected to MySQL Server version ", mysql_connection.get_server_info())

	def create_validated_database(self):
		"""
		Create validated database.
		Return connection to new database
		"""
		host = self.schema_conv_init_option.host
		username = self.schema_conv_init_option.username
		password = self.schema_conv_init_option.password
		# validated_dbname = self.schema_conv_init_option.dbname + "_validated"
		mydb = open_connection_mysql(host, username, password)
		mycursor = mydb.cursor()
		mycursor.execute("SHOW DATABASES")
		mysql_table_list = [fetched_data[0] for fetched_data in mycursor]
		
		if self.validated_dbname in mysql_table_list:
			mycursor.execute(f"DROP DATABASE {self.validated_dbname}")
		
		mycursor.execute(f"CREATE DATABASE {self.validated_dbname}")
		mycursor.close()
		mydb.close()
		print("Disconnected to MySQL Server version ", mydb.get_server_info())
		mydb = open_connection_mysql(host, username, password, self.validated_dbname)
		print("Create validated table successfully!")
		return mydb


	def get_tables_creating_info(self):
		"""
		*Get tables only, not views
		Dict(
			key: <table uuid>, 
			value: Dict(
				key: "schema", value: <table name>,
				key: "name", value: <table name>,
				key: "engine", value: <table engine>,
				key: "charset", value: <table charset>,
			)
		)
		"""
		pass

	def get_constraints_creating_info(self):
		pass

	def get_columns_info(self):
		"""
		List[
			Dict(
				key: "uuid", value: <column uuid>,
				key: "column-name", value: <column name>,
				key: "table-name", value: <table name>,
				key: "column-type", value: <column type>,
				key: "auto-incremented", value: <auto incremented option>,
				key: "nullable", value: <nullable option>,
				key: "default-value", value: <default value>,
				key: "column-width", value: <column width>,
			)
		]
		"""
		db_schema = self.schema.get()
		columns_info_list = []
		for column_schema in db_schema["all-table-columns"]:
			table_name, column_name = column_schema["short-name"].split(".")[:2]
			column_info = {
				"uuid": column_schema["@uuid"],
				"column-name": column_name,
				"table-name": table_name,
				"column-type": column_schema["attributes"]["COLUMN_TYPE"],
				"auto-incremented": column_schema["auto-incremented"],
				"nullable": column_schema["nullable"],
				"default-value": self.get_column_default_value(column_schema),
				"column-width": column_schema["width"],
			}
			columns_info_list.append(column_info)
		return columns_info_list


	def get_column_default_value(self, column_schema):
		prefix_suffix_col_dtype_list = self.get_prefix_suffix_column_data_types_list()
		# print(prefix_suffix_col_dtype_list)
		return
		prefix = ""
		suffix = ""
		for col_dtype in prefix_suffix_col_dtype_list:
			if column_schema["column-data-type"] == col_dtype["uuid"]:
				prefix = col_dtype["prefix"]
				suffix = col_dtype["suffix"]
				res = prefix + column_schema["default-value"] + suffix
				return res
		return column_schema["default-value"]

	def get_prefix_suffix_column_data_types_list(self):
		db_schema = self.schema.get()
		# Get prefix and suffix of default value
		## Get all column-data-type which have prefix and suffix
		column_data_types_list = []
		for column_schema in db_schema["all-table-columns"]:
			if type(column_schema["column-data-type"]) is dict:
				if "literal-prefix" in column_schema["column-data-type"].keys():
					prefix = column_schema["column-data-type"]["literal-prefix"]
				else:
					prefix = None
				if "literal-suffix" in column_schema["column-data-type"].keys():
					suffix = column_schema["column-data-type"]["literal-suffix"]
				else:
					suffix = None
				if prefix is not None and suffix is not None:
					prefix_suffix_info = {
						"uuid": column_schema["column-data-type"]["@uuid"],
						"prefix": column_schema["column-data-type"]["literal-prefix"], 
						"suffix": column_schema["column-data-type"]["literal-suffix"] 
					}
					column_data_types_list.append(prefix_suffix_info)
		return column_data_types_list


	def create_validated_tables(self, mysql_connection):
		db_schema = self.schema.get()
		table_info_list = self.get_table_info_list()
		for table_info in table_info_list:
			self.create_one_table(mysql_connection, table_info)
		### alter table
		pass

	def create_one_table(self, mysql_connection, table_info):
		columns_info_list = list(filter(lambda column_info: column_info["uuid"] in table_info["columns-uuid-list"], self.get_columns_info()))
		primary_key_info = list(filter(lambda index_info: index_info["uuid"] == table_info["primary-key-uuid"], self.get_primary_indexes_info_list()))[0] 

		#sql create column
		sql_creating_columns_cmd = ",\n".join([self.generate_sql_creating_column(column_info) for column_info in columns_info_list])
		#sql create key
		sql_creating_key_cmd = self.generate_sql_creating_key(primary_key_info)

		sql_creating_columns_and_key_cmd = sql_creating_columns_cmd + ",\n" + sql_creating_key_cmd 
		#sql create table
		sql_creating_table_cmd = f"""CREATE TABLE {table_info["table-name"]} (\n{sql_creating_columns_and_key_cmd}\n) ENGINE={table_info["engine"]} DEFAULT CHARSET={table_info["table-collation"]};"""
		# print(sql_creating_table_cmd)
		
		# create table
		mycursor = mysql_connection.cursor()
		mycursor.execute(sql_creating_table_cmd)
		mycursor.close()

	def generate_sql_creating_column(self, column_info):
		"""
		Generate SQL command for creating column from this column info dict:
		Dict(
				key: "uuid", value: <column uuid>,
				key: "column-name", value: <column name>,
				key: "table-name", value: <table name>,
				key: "column-type", value: <column type>,
				key: "column-width", value: <column width>,
				key: "nullable", value: <nullable option>,
				key: "default-value", value: <default value>,
				key: "auto-incremented", value: <auto incremented option>,
			)
		"""
		sql_cmd_list = []
		sql_cmd_list.append(column_info["column-name"])
		creating_data_type = self.parse_mysql_data_type(column_info["column-type"], column_info["column-width"])
		sql_cmd_list.append(creating_data_type)
		if column_info["nullable"] is False:
			sql_cmd_list.append("NOT NULL")
		if column_info["default-value"] is not None:
			sql_cmd_list.append(f"""default {column_info["default-value"]}""")
		if column_info["auto-incremented"] is True:
			sql_cmd_list.append("AUTO_INCREMENT")

		sql_cmd = " ".join(sql_cmd_list)
		return sql_cmd

	def generate_sql_creating_key(self, primary_key_info):
		"""
		Generate SQL creating key command from primary key info dict like this:
		Dict(
				key: "uuid", value: <index uuid>,
				key: "table-name", value: <table name>,
				key: "columns-uuid-list", value: <columns uuid list>,
				key: "unique", value: <unique option>,
			)
		"""
		coluuid_colname_dict = self.schema.get_columns_dict()
		columns_in_pk_list = [coluuid_colname_dict[col_uuid] for col_uuid in primary_key_info["columns-uuid-list"]]
		sql_creating_key = f"""PRIMARY KEY ({", ".join(columns_in_pk_list)})"""
		return sql_creating_key


	def parse_mysql_data_type(self, dtype, width):
		"""
		Generate MySQL data type for creating column SQL command.
		Params:
			-dtype: MySQL data type and unsigned option
			-width: length of column
		"""
		mysql_type_list = [
			"tinyint", "smallint", "mediumint", "int", "integer", "bigint",
			"decimal", "dec", "fixed",
			"float", "double", "real",
			"bool", "boolean",
			"date", "year",
			"datetime", "timestamp", "time",
			"bit", "binary", "varbinary",
			"tinyblob", "blob", "mediumblob", "longblob",
			"character", "charset", "ascii", "unicode", "char", "varchar", "tinytext", "text", "mediumtext", "longtext",
			"enum", "set",
			"geometry", "point", "linestring", "polygon",
			"multipoint", "multilinestring", "multipolygon", "geometrycollection"
		]
		for mysql_type in mysql_type_list:
			if re.search(f"^{mysql_type}", dtype):
				# Handle enum data type
				if mysql_type == "enum":
					return dtype
				elif mysql_type == "set":
					return dtype
				else:
					if bool(re.search(f"unsigned$", dtype)):
						unsigned = " unsigned"
					else:
						unsigned = ""
					res = mysql_type + width + unsigned
					return res
		return None


	def get_table_info_list(self):
		"""
		List[
			Dict(
				key: "uuid", value: <table uuid>,
				key: "table-name", value: <table name>,
				key: "engine", value: <engine>,
				key: "table-collation", value: <table collation>,
				key: "columns-uuid-list", value: <List of columns uuid>,
				key: "primary-key-uuid", value: <primary key uuid>,

			)
		]
		Not be handled yet. Will be handled in next phase:
				key: "foreign key", value: ???,
				key: "table-constraints", value: ???,
				key: "indexes", value: ???, ### may be neccesary or not, because primary key is auto indexed
		"""
		table_info_list = []
		db_schema = self.schema.get()
		for table_schema in db_schema["catalog"]["tables"]:
			if self.get_table_type(table_schema["table-type"]) == "TABLE":
				table_info = {
					"uuid": table_schema["@uuid"],
					"table-name": table_schema["name"],
					"engine": table_schema["attributes"]["ENGINE"],
					"table-collation": table_schema["attributes"]["TABLE_COLLATION"].split("_")[0],
					"columns-uuid-list": table_schema["columns"],
					"primary-key-uuid": table_schema["primary-key"]
				}
				table_info_list.append(table_info)
		return table_info_list

	def get_table_type(self, table_type):
		"""
		Define table type is TABLE or VIEW.
		Parameter:
			-table_type: table type which was get from schema, either be object or string
		"""
		# Dict(key: <table type uuid>, value: <table type>)
		if type(table_type) is dict:
			return table_type["table-type"]
		else:
			table_type_dict = {}
			db_schema = self.schema.get()
			for table_schema in db_schema["catalog"]["tables"]:
				if type(table_schema["table-type"]) is dict:
					table_type_dict[table_schema["table-type"]["@uuid"]] = table_schema["table-type"]["table-type"]
			return table_type_dict[table_type]

	def get_primary_indexes_info_list(self):
		"""
		Get only index on primary keys of tables. 
		Use for defining primary key when creating table.
		Index info:
			Dict(
				key: "uuid", value: <index uuid>,
				key: "table-name", value: <table name>,
				key: "columns-uuid-list", value: <columns uuid list>,
				key: "unique", value: <unique option>,
			)
		"""
		db_schema = self.schema.get()
		indexes_info_list = []
		for table_schema in db_schema["catalog"]["tables"]:
			for index in table_schema["indexes"]:
				if type(index) is dict:
					if index["name"] == "PRIMARY":
						index_info = {
							"uuid": index["@uuid"],
							"table-name": index["attributes"]["TABLE_NAME"],
							"columns-uuid-list": index["columns"],
							"unique": index["unique"]
						}
						indexes_info_list.append(index_info)
		return indexes_info_list

	def migrate_mongodb_to_mysql(self):
		pass

	def evaluate_validating(self):
		pass	

	def find_target_dtype(self, mysql_dtype, dtype_dict, mongodb_dtype):
		"""
		Mapping data type from MySQL to MongoDB.
		Just use this function for migrate_mysql_to_mongodb function
		"""
		for target_dtype in dtype_dict.keys():
			if(mysql_dtype) in dtype_dict[target_dtype]:
				return mongodb_dtype[target_dtype]
		return None 

	def migrate_mysql_to_mongodb(self):
		"""
		Migrate data from MySQL to MongoDB.
		"""

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

		try:
			db_connection = open_connection_mysql(
				self.schema_conv_init_option.host, 
				self.schema_conv_init_option.username, 
				self.schema_conv_init_option.password, 
				self.schema_conv_init_option.dbname, 
			)
			if db_connection.is_connected():
				#start migrating
				#read table_column_dtype_dict
				table_column_dtype_dict = self.schema.get_table_column_and_data_type()
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
							# sql_cmd = sql_cmd + " ST_AsGeoJSON(" + col + "),"
							# sql_cmd = sql_cmd + " AsWKT(" + col + "),"
						else:
							sql_cmd = sql_cmd + " `" + col + "`,"
					#join sql
					sql_cmd = sql_cmd[:-1] + " FROM " + table
					db_cursor = db_connection.cursor();
					#execute sql
					db_cursor.execute(sql_cmd)
					#fetch data and convert ###NOT CONVERT YET
					fetched_data = db_cursor.fetchall()
					rows = []
					for row in fetched_data:
						data = {}
						for i in range(len(col_fetch_seq)):
							col = col_fetch_seq[i]
							dtype = table_column_dtype_dict[table][col]
							target_dtype = self.find_target_dtype(dtype, dtype_dict, mongodb_dtype)
							#generate SQL
							if row[i] != None:
								if dtype == "GEOMETRY":
									geodata = [float(num) for num in row[i][6:-1].split()]
									geo_x, geo_y = geodata[:2]
									if geo_x > 180 or geo_x < -180:
										geo_x = 0
									if geo_y > 90 or geo_y < -90:
										geo_y = 0
									converted_data = {
										"type": "Point",
										"coordinates": [geo_x, geo_y]
									}
								elif dtype == "VARCHAR":
									converted_data = str(row[i])
								elif dtype == "BIT":
									###get col type from schema attribute 
									mysql_col_type = self.schema.get_col_type_from_schema_attribute(table, col)
									if mysql_col_type == "tinyint(1)":
										binary_num = row[i]
										converted_data = binary_num.to_bytes(len(str(binary_num)), byteorder="big")
									else:
										converted_data = row[i]
								# elif dtype == "YEAR":
									# print(row[i], type(row[i]))
								elif target_dtype == mongodb_dtype["decimal"]:
									converted_data = Decimal128(row[i])
								elif target_dtype == mongodb_dtype["object"]:
									if type(row[i]) is str:
										converted_data = row[i]
									else:
										converted_data = tuple(row[i])
								else:
									converted_data = row[i]
								data[col_fetch_seq[i]] = converted_data 
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


	# def migrate_json_to_mongodb(self):
	# 	"""
	# 	Migrate data from json file to MongoDB.
	# 	"""
	# 	db_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname)
	# 	tables_and_views_name_list = self.schema.get_tables_and_views_list()
	# 	for table_name in tables_and_views_name_list:
	# 		collection_name = table_name
	# 		json_filename = collection_name + ".json"
	# 		import_json_to_mongodb(db_connection, collection_name, self.schema_conv_output_option.dbname, json_filename, True)
	# 	print("Migrate data from JSON to MongoDB successfully!")

	def convert_relations_to_references(self):
		"""
		Convert relations of MySQL table to database references of MongoDB
		"""
		tables_name_list = self.schema.get_tables_name_list()
		# db_connection = open_connection_mongodb(mongodb_connection_info)
		tables_relations = self.schema.get_tables_relations()
		# converting_tables_order = specify_sequence_of_migrating_tables(schema_file)
		edited_table_relations_dict = {}
		original_tables_set = set([tables_relations[key]["primary_key_table"] for key in tables_relations])

		# Edit relations of table dictionary
		for original_table in original_tables_set:
			for key in tables_relations:
				if tables_relations[key]["primary_key_table"] == original_table:
					if original_table not in edited_table_relations_dict.keys():
						edited_table_relations_dict[original_table] = []
					edited_table_relations_dict[original_table] = edited_table_relations_dict[original_table] + [extract_dict(["primary_key_column", "foreign_key_table", "foreign_key_column"])(tables_relations[key])]
		# Convert each relation of each table
		for original_collection_name in tables_name_list:
			if original_collection_name in original_tables_set:
				for relation_detail in edited_table_relations_dict[original_collection_name]:
					referencing_collection_name = relation_detail["foreign_key_table"]
					original_key = relation_detail["primary_key_column"]
					referencing_key = relation_detail["foreign_key_column"]
					self.convert_one_relation_to_reference(original_collection_name, referencing_collection_name, original_key, referencing_key) 
		print("Convert relations successfully!")


	def convert_one_relation_to_reference(self, original_collection_name, referencing_collection_name, original_key, referencing_key):
		"""
		Convert one relation of MySQL table to database reference of MongoDB
		"""
		db_connection = open_connection_mongodb(self.schema_conv_output_option.host, self.schema_conv_output_option.port, self.schema_conv_output_option.dbname)
		original_collection_connection = db_connection[original_collection_name]
		original_documents = original_collection_connection.find()
		new_referenced_key_dict = {}
		for doc in original_documents:
			new_referenced_key_dict[doc[original_key]] = doc["_id"]

		referencing_documents = db_connection[referencing_collection_name]
		for key in new_referenced_key_dict:
			new_reference = {}
			new_reference["$ref"] = original_collection_name
			new_reference["$id"] = new_referenced_key_dict[key]
			new_reference["$db"] = self.schema_conv_output_option.dbname
			referencing_key_new_name = "db_ref_" + referencing_key
			referencing_documents.update_many({referencing_key: key}, update={"$set": {referencing_key_new_name: new_reference}})

	
