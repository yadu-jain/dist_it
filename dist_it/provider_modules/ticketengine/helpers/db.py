#Author: Heera
#Date: 2014-08-27
#Description: DB Adapter

import pymssql

class DB(object):
	"""
		Author: Heera
		Date: 2014-08-27
		Description: Process stations data from api, param=str_response
	"""
	def __init__(self,server,db,user,password):
		self.server		= server
		self.db 		= db
		self.user 		= user
		self.password 	= password

	def execute_query(self,str_query):
		result=None
		with pymssql.connect(self.server, self.user, self.password, self.db) as conn:
			with conn.cursor(as_dict=True) as cursor:
				cursor.execute(str_query)
				result=cursor.fetchall()
		return result

	def execute_dml_bulk(self,str_query,list_data):
		with pymssql.connect(self.server, self.user, self.password, self.db) as conn:
			with conn.cursor(as_dict=True) as cursor:
				cursor.executemany(str_query,list_data)				
				conn.commit()
	
	def execute_dml(self,str_query):
		with pymssql.connect(self.server, self.user, self.password, self.db) as conn:
			with conn.cursor(as_dict=True) as cursor:
				cursor.execute(str_query)				
				conn.commit()				

	def execute_sp(self,sp_name,list_params):
		result=None
		with pymssql.connect(self.server, self.user, self.password, self.db) as conn:
			with conn.cursor(as_dict=True) as cursor:
				result=cursor.callproc(sp_name,list_params)
		return result

if __name__=='__main__':
	db=DB("pulldb3","Pull_Parveen_2","agn","t0w3r47556br!dg3")
	print db.execute_query("""select * 
							from test with (nolock)""")
	db.execute_dml_bulk("insert into test(sr_no,name,value) values(%d,%s,%d)",[(123,'Heera',1)])
	print db.execute_query("select * from test with (nolock)")
	pass

