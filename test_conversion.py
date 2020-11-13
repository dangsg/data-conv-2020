import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient

def mysql_list_id(table_name, col_name):
    print("Reading BLOB data from python_employee table")

    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='sakila',
                                             user='dangsg',
                                             password='Db@12345678')

        cursor = connection.cursor()
        sql_fetch_blob_query = f"SELECT {col_name} from {table_name};"

        cursor.execute(sql_fetch_blob_query)
        record = cursor.fetchall()
        # for row in record:
            # print(record)
        res = [row[0] for row in record]
        return res
    except mysql.connector.Error as error:
        print("Failed to read BLOB data from MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# readBLOB()
def mongodb_list_id(document_name, field_name):
    connection_string = f"mongodb://localhost:27017/"
    try:
        # Making connection 
        mongo_client = MongoClient(connection_string)  
        # Select database  
        db_connection = mongo_client["sakila"]        
        col = db_connection[document_name]
        docs = col.find()
        data = [doc[field_name] for doc in docs]
        return data
    except Exception as e:
        print("Error while connecting to MongoDB", e)
        raise e
# mongodb_binary()

print(len(mysql_list_id("rental", "rental_id")))
print(len(mongodb_list_id("rental", "rental_id")))
