# utilities.py: Utitilies functions which are used for conversion processes.

import mysql.connector
from pymongo import MongoClient
import json 
import sys
 		 
def extract_dict(selected_keys):
	"""
	Extract selected-by-key fields from dict.
	This function is used as iteration of Python map() function.
	"""
	def extract_dict(input_dict):
		output_dict = {}
		for key in selected_keys:
			output_dict[str(key)] = input_dict[str(key)]
		return output_dict
	return extract_dict
  
def import_json_to_mongodb(db_connection, collection_name, dbname, json_filename, import_dataset=False):
	"""
	Import intermediate JSON file, which was generated and saved at path "./intermediate/<database-name>", into MongoDB.
	Parameter:
		import_dataset:
			- True: If you want to import a dataset (MySQL table / Mongodb collection). 
			- False: If you want to import a schema file (was usually generated by SchemaCrawler).
	"""
	try:		   
		# Created or switched to collection  
		Collection = db_connection[collection_name]
		# Opening json file 
		with open(f"./intermediate_data/{dbname}/{json_filename}") as file: 
		    file_data = json.load(file) 
		    table_data = file_data
		    if import_dataset is True:
			    table_data = file_data["data"]
		    # else:
		    	# table_data = {
		    		# "auto_generated_schema": file_data
		    	# }

		# Inserting the json data in the Collection 
		# If JSON contains data more than one entry 
		# insert_many is used else inser_one is used 
		if isinstance(table_data, list): 
			Collection.insert_many(table_data, ordered=False)   
		else: 
			Collection.insert_one(table_data) 
		return True
		print(f"Write data from JSON file {json_filename} to MongoDB collection {collection_name} of database {dbname} successfully!") 
	except Exception as e:
		print(f"Error while writing JSON file {json_filename} to MongoDB collection {collection_name} of database {dbname}")
		print(e)
		raise e

def store_json_to_mongodb(mongodb_connection, collection_name, json_data):
	"""
	Import data from JSON object (not from JSON file).
	"""
	try:		   
		# Created or switched to collection  
		Collection = mongodb_connection[collection_name]
		if isinstance(json_data, list): 
		    Collection.insert_many(json_data, ordered=False)   
		else: 
		    Collection.insert_one(json_data)
		print(f"Write JSON data to MongoDB collection {collection_name} successfully!") 
		return True
	except Exception as e:
		print(f"Error while writing data to MongoDB collection {collection_name}.")
		print(e)
		raise e

def drop_mongodb_database(host, port, dbname):
	"""
	Drop MongoDB database.
	Be useful, just use this function at the begining of conversion.
	"""
	connection_string = f"mongodb://{host}:{port}/"
	try:
		# Making connection 
		mongo_client = MongoClient(connection_string)  
		mongo_client.drop_database(dbname)
		return True
	except Exception as e:
		print(f"Error while dropping MongoDB database {dbname}! Re-check connection or name of database.")
		print(e)
		raise e

def open_connection_mongodb(host, port, dbname):
	"""
	Set up a connection to MongoDB database.
	Return a MongoClient object if success.
	"""
	connection_string = f"mongodb://{host}:{port}/"
	try:
		# Making connection 
		mongo_client = MongoClient(connection_string)  
		# Select database  
		db_connection = mongo_client[dbname] 		
		return db_connection
	except Exception as e:
		print(f"Error while connecting to MongoDB database {dbname}! Re-check connection or name of database.")
		print(e)
		raise e

def load_mongodb_collection(host, port, dbname, collection_name):
	"""
	Load all documents from MongoDB collection.
	"""
	mongodb_connection = open_connection_mongodb(host, port, dbname)
	collection = mongodb_connection[collection_name]
	docs = collection.find()
	res = [doc for doc in docs]
	return res

def open_connection_mysql(host, username, password, dbname = None):
	"""
	Set up a connection to MySQL database.
	Return a MySQL (connector) connection object if success, otherwise None.
	"""
	try:
		db_connection = mysql.connector.connect(
			host = host, 
			user = username, 
			password = password, 
			database = dbname
		)
		if db_connection.is_connected():
			db_info = db_connection.get_server_info()
			print("Connected to MySQL Server version ", db_info)

			return db_connection
		else:
			print("Connect fail!")
			return None
	except Exception as e:
		print(f"Error while connecting to MySQL database {dbname}! Re-check connection or name of database.")
		print(e)
		raise e

	# def write_json_to_file(self, json_data, filename):
	# 	"""Write json data to file"""
	# 	with open(f"./intermediate_data/{self.schema_conv_init_option.dbname}/{filename}", 'w') as outfile:
	# 		json.dump(json_data, outfile, default=str)

if __name__ == '__main__':
	mongodb_host = 'localhost'
	mongodb_username = ''
	mongodb_password = ''
	mongodb_port = '27017'
	mongodb_dbname = 'sakila'
	load_mongodb_collection(mongodb_host, mongodb_port, mongodb_dbname, "original_schema")