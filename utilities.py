import mysql.connector
from pymongo import MongoClient
import json 
import sys
 		 
def extract_dict(selected_keys):
	def extract_dict(input_dict):
		output_dict = {}
		for key in selected_keys:
			output_dict[str(key)] = input_dict[str(key)]
		return output_dict
	return extract_dict
  
def import_json_to_mongodb(db_connection, collection_name, dbname, json_filename, import_dataset=False):
	try:		   
		# Created or switched to collection  
		Collection = db_connection[collection_name]
		Collection.drop() 
		  
		# Loading or Opening the json file 
		with open(f"./intermediate_data/{dbname}/{json_filename}") as file: 
		    file_data = json.load(file) 
		    table_data = file_data
		    if import_dataset is True:
			    table_data = file_data["data"]

		# Inserting the loaded data in the Collection 
		# if JSON contains data more than one entry 
		# insert_many is used else inser_one is used 
		if isinstance(table_data, list): 
		    Collection.insert_many(table_data)   
		else: 
		    Collection.insert_one(table_data) 
	except Exception as e:
		print("Error while writing to MongoDB", e)
		raise e

def store_json_to_mongodb(mongodb_connection, collection_name, json_data):
	try:		   
		# Created or switched to collection  
		Collection = mongodb_connection[collection_name]
		# Write new data
		Collection.drop() 
		# Inserting the loaded data in the Collection 
		# if JSON contains data more than one entry 
		# insert_many is used else inser_one is used 
		if isinstance(json_data, list): 
		    Collection.insert_many(json_data)   
		else: 
		    Collection.insert_one(json_data)
		print("Write data to MongoDB successfully!") 
	except Exception as e:
		print("Error while writing to MongoDB", e)
		raise e

def open_connection_mongodb(host, port, dbname):
	connection_string = f"mongodb://{host}:{port}/"
	try:
		# Making connection 
		mongo_client = MongoClient(connection_string)  
		# Select database  
		db_connection = mongo_client[dbname] 		
		return db_connection
	except Exception as e:
		print("Error while connecting to MongoDB", e)
		raise e

def open_connection_mysql(connection_info):
	try:
		db_connection = mysql.connector.connect(
			host=connection_info["host"], 
			user=connection_info["username"], 
			password=connection_info["password"], 
			database=connection_info["database"]
		)
		if db_connection.is_connected():
			db_info = db_connection.get_server_info()
			print("Connected to MySQL Server version ", db_info)

			return db_connection
		else:
			print("Connect fail!")
			return Null
	except Exception as e:
		print("Error while connecting to MySQL", e)