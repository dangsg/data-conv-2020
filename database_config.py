

class ConvInitOption:
	"""docstring for DataConvInitOption"""
	def __init__(self, host, username, password, port, dbname):
		super(ConvInitOption, self).__init__()
		self.host = host
		self.username = username
		self.password = password
		self.port = port
		# self.dbtype = dbtype = "MySQL"
		self.dbname = dbname

class ConvOutputOption:
	"""docstring for DataConvOutputOption"""
	def __init__(self, host, username, password, port, dbname):
		super(ConvOutputOption, self).__init__()
		self.host = host
		self.username = username
		self.password = password
		self.port = port
		# self.dbtype = dbtype = "MongoDB"
		self.dbname = dbname		
		