class Provider_Exception(Exception):
	"""docstring for Process_Exc"""
	def __init__(self,reason):
		self.value = "Config Load Exception|Reason: "+reason
	def __str__(self):
		return self.value

class Config_Load_Exc(Provider_Exception):
	"""docstring for Process_Exc"""
	def __init__(self,reason):
		self.value = "Config Load Exception|Reason: "+reason

class Pull_Exc(Provider_Exception):
	"""docstring for Process_Exc"""
	def __init__(self,reason):
		self.value ="Data Pull Exception|Reason: "+reason

class Process_Exc(Provider_Exception):
	"""docstring for Process_Exc"""
	def __init__(self,reason):
		self.value ="Data Process Exception|Reason: "+reason
						