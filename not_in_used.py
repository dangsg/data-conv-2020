def specify_sequence_of_migrating_tables(schema_file):
		"""
		Specify sequence of migrating tables from MySQL. The sequence must guarantee all tables and data within them will be migrated effectively and efficiently.
		We will make a tree to determine which order each table should have.
		Result will be a dictionary which have tables' names as key and orders in sequence as values.
		The lower mark table have, the higher order get, and data of it will be migrate previously.
		"""
		db_schema = read_schema_file(schema_file)
		tables_schema = get_tables_schema(db_schema)
		extracted_tables_schema = extract_tables_schema(tables_schema)
		tables_relations = get_tables_relations(schema_file)
		tables_name_list = get_tables_name_list(schema_file)

		refering_tables_set = set(map(lambda ele: ele["dest-table"], tables_relations.values()))
		root_nodes = set(tables_name_list) - refering_tables_set

		node_seq = dict.fromkeys(tables_name_list, -1)
		node_seq.update(dict.fromkeys(root_nodes, 0))

		# Eliminate self reference relation
		tables_relations_list = list(filter(lambda rel: rel["source-table"] != rel["dest-table"], list(tables_relations.values())))

		current_mark = 0
		while(current_mark <= max(node_seq.values())):
			source_nodes = set(filter(lambda key: node_seq[str(key)] == current_mark, node_seq.keys()))
			for source_node in source_nodes:
				for direction in tables_relations_list:
					if(direction["source-table"] == source_node):
						if(node_seq[direction["dest-table"]] < current_mark + 1):
							node_seq[direction["dest-table"]] = current_mark + 1
						direction["source-table"] = None #TODO: Find a more effective way to eliminate retrieved nodes
			current_mark = current_mark + 1

		table_seq = {} 
		for i in range(current_mark):
			table_seq[str(i)] = []
			for key in list(node_seq):
				if node_seq[key] == i:
					table_seq[str(i)] = table_seq[str(i)] + [key]
		return table_seq 

def fetch_table_rows(self, db_connection, table_name):
		"""
		Fetch all rows of specific table
		"""
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
				data = self.convert_to_bson(row[i], columns[i])
				row_dict[columns[i]] = data
			dataset.append(row_dict)
		json_data = {}
		json_data["table"] = table_name
		json_data["data"] = dataset
		return json_data

	def convert_to_bson(self, data, col):
		if type(data) is bytearray:
			print(data, col)
		return data