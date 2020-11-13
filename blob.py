import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient
from bson import Decimal128
import datetime

def write_file(data, filename):
    # Convert binary data to proper format and write it on Hard Disk
    with open(filename, 'wb') as file:
        file.write(data)

def readBLOB():
    print("Reading BLOB data from python_employee table")

    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='sakila',
                                             user='dangsg',
                                             password='Db@12345678')

        cursor = connection.cursor()
        sql_fetch_blob_query = """SELECT * from staff where staff_id = 1"""

        cursor.execute(sql_fetch_blob_query)
        record = cursor.fetchall()
        for row in record:
            print("Id = ", row[0], )
            print("Name = ", row[1])
            image = row[4]
            # file = row[3]
            # print("Storing employee image and bio-data on disk \n")
            write_file(image, "data")
            print(type(image))
            return image
            # write_file(file, bioData)

    except mysql.connector.Error as error:
        print("Failed to read BLOB data from MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# readBLOB()
def mongodb_binary():
    connection_string = f"mongodb://localhost:27017/"
    try:
        # Making connection 
        mongo_client = MongoClient(connection_string)  
        # Select database  
        db_connection = mongo_client["sakila3"]        
        col = db_connection["staff"]
        # for doc in col.find({"staff_id": "1"}):
        # for doc in col.find():
            # print(1)
            # print(doc["picture"])
        doc = col.find_one({"staff_id": "1"})
        data = doc["picture"]
        write_file(data, "mongo")
        return data
    except Exception as e:
        print("Error while connecting to MongoDB", e)
        raise e
# mongodb_binary()

def update_mongo():
    connection_string = f"mongodb://localhost:27017/"
    try:
        # Making connection 
        mongo_client = MongoClient(connection_string)  
        # Select database  
        db_connection = mongo_client["sakila3"]        
        col = db_connection["staff"]
        pic = readBLOB()
        mydict = {}
        mydict["picture"] = pic
        # col.update_one({"staff_id": "1"}, {"$set":{"picture": pic}})
        col.update_one({"staff_id": "1"}, {"$set":mydict})
        # for doc in col.find({"staff_id": "1"}):
        # for doc in col.find():
            # print(1)
            # print(doc["picture"])
        doc = col.find_one({"staff_id": "1"})
        data = doc["picture"]
        return data
        # write_file(data, "mongo")
    except Exception as e:
        print("Error while connecting to MongoDB", e)
        raise e

# update_mongo()
# my = readBLOB()
# mon = update_mongo()
# print(my == mon)
# print(type(my), type(mon))
# print(len(my), len(mon))
# print(my)

def read_loc():
    print("Reading BLOB data from python_employee table")

    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='sakila',
                                             user='dangsg',
                                             password='Db@12345678')

        cursor = connection.cursor()
        #sql_fetch_blob_query = """SELECT ST_AsBinary(location) from address where address_id = 1"""
        sql_fetch_blob_query = """SELECT ST_X(location), ST_Y(location) from address where address_id = 1"""

        cursor.execute(sql_fetch_blob_query)
        record = cursor.fetchall()
        for row in record:
            # print("Id = ", row[0], )
            # print("Name = ", row[1])
            loc = row
            # file = row[3]
            # print("Storing employee image and bio-data on disk \n")
            # write_file(image, "data")
            print(type(loc), loc)
            return loc
            # print(loc[0][6:-1].split())
            # return image
            # write_file(file, bioData)

    except mysql.connector.Error as error:
        print("Failed to read BLOB data from MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def mongodb_point():
    connection_string = f"mongodb://localhost:27017/"
    try:
        # Making connection 
        mongo_client = MongoClient(connection_string)  
        # Select database  
        db_connection = mongo_client["sakila3"]        
        col = db_connection["address"]
        loc = read_loc()
        col.update_one({"address_id": "1"}, {"$set": {
            "location":
            { "type": "Point", "coordinates": [ loc[0], loc[1] ] }
            }
            })
        for doc in col.find({"address_id": "1"}):
        # for doc in col.find():
            # print(1)
            print(doc)
        # doc = col.find_one({"staff_id": "1"})
        # data = doc["picture"]
        # return data
        # write_file(data, "mongo")
    except Exception as e:
        print("Error while connecting to MongoDB", e)
        raise e
# mongodb_point()

def check_validation():
    connection_string = f"mongodb://localhost:27017/"
    try:
        # Making connection 
        mongo_client = MongoClient(connection_string)  
        # Select database  
        db_connection = mongo_client["sakila"]        
        col = db_connection["film"]
        document = {
        'film_id': 12112, 
        'title': 'ACADEMY DINOSAUR', 
        'description': 'A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies', 
        'release_year': 2006, 
        'language_id': 1, 
        'original_language_id': 0, 
        'rental_duration': 6, 
        'rental_rate': Decimal128('0.99'), 
        'length': 86, 
        'replacement_cost': Decimal128('20.99'), 
        'rating': 'PG', 
        'special_features': ['Behind the Scenes', 'Deleted Scenes'], 
        'last_update': datetime.datetime(2006, 2, 15, 5, 3, 42)
        }
        col.insert_one(document)
    except Exception as e:
        print("Error while connecting to MongoDB", e)
        raise e

check_validation()