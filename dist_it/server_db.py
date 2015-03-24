#Author: Heera
#Date: 2015-03-23
#Description: Library to store server data INTEGERo db

import sqlite3 as sqlite
import multiprocessing as mp
from multiprocessing import Lock
import os
import threading
import json
from datetime import datetime

conn_name="D:/repo/dist_it_new/env/dist_it/dist_it/data/server_db.db"

# class Persistant_Queue(Queue):
# 	def __init__(self):
# 		self.sqlite_conn=Jobs_Persistance()		
# 		print self.sqlite_conn
# 		print os.getpid()
# 		super(Persistant_Queue, self).__init__()		

# 	def put(self,obj,**kwrd):
# 		print "Got it"
# 		print obj
# 		print os.getpid()
# 		#self.sqlite_conn.add_job(obj[0],obj[1])
# 		super(Persistant_Queue, self).put(obj,**kwrd)

class Jobs_Persistance(object):
	"""
	"""
	def __init__(self,load_previous=False):		
		try:			
			print "init"
			self.sqlite_conn=sqlite.connect(conn_name,check_same_thread=False)
			to_create=False
			try:
				with self.sqlite_conn:
					cur=self.sqlite_conn.cursor()
					cur.execute(
						"""
						SELECT COUNT(*) total FROM jobs WHERE is_done=0    			

						"""
					)
					total_counts=cur.fetchone()[0]
					cur.close()
					print "Total pending jobs= %d" % total_counts
					##TODO: load jobs in server
			except Exception as e:
				print e
				print "Creating jobs persitance..."
				with self.sqlite_conn:
					cur=self.sqlite_conn.cursor()
					cur.executescript(
						"""
						CREATE TABLE jobs
						(
							id INTEGER PRIMARY KEY AUTOINCREMENT,
							job_module TEXT,
							job_fun TEXT,
							params TEXT,
							callbacks TEXT,
							is_success INTEGER,	    					
							job_type TEXT,
							created_on TIMESTAMP,	    					
							is_queued INTEGER,			
							is_done INTEGER,	    					
							done_on TIMESTAMP,	    
							assigned_to TEXT,
							exec_time INTEGER
						)

						"""	    				
					)
					cur.close()			
		except Exception as e:
			print e
			raise Exception("Failed to connect sqlite db"+": "+str(e))
		self.lock = Lock()
			
	def add_job(self,job,callback_list,job_type="DEFAULT"):		
		print os.getpid()
		print threading.current_thread()
		query="""
		INSERT INTO jobs
		(
			job_module,
			job_fun,
			params,
			callbacks,
			is_success,
			job_type,			
			created_on,
			is_queued,			
			is_done,
			done_on,
			assigned_to,
			exec_time			
		)
		VALUES
		(			
			%s
		)
		""" % ("?," * 12)[:-1]	

		params=[
			job[0],
			job[1],
			json.dumps(job[2]),
			json.dumps({"callbacks":callback_list}),			
			0,
			job_type,
			datetime.now(),
			0,
			0,
			None,
			None,
			None
		]

		row_id=None
		with self.lock:
		#print self.lock
			try:
				with self.sqlite_conn:
					cur = self.sqlite_conn.cursor() 
					cur.execute(query, params) 
					#rows = cur.fetchall()
					row_id = cur.lastrowid
					cur.close()							
					self.sqlite_conn.commit()
			except Exception as e:
				print e
				pass
			return row_id

	def job_queued(self,job):
		row_id=job[3]
		if row_id==None:
			return
		query="""
			UPDATE jobs SET is_queued=1 WHERE id=?
		"""
		params=[row_id]
		with self.lock:			
			try:
				with self.sqlite_conn:
					cur=self.sqlite_conn.cursor()
					cur.execute(query,params)
					cur.close()
			except Exception as e:
				print e
				#raise e
	
	def job_done(self,response):
		print os.getpid()
		job=response["request"]
		total_time=int(response["total_time"])
		row_id=job[3]
		if row_id==None:
			return
		is_success=int(response["success"])
		query="""
			UPDATE jobs SET 
				is_success=?,
				is_done=1,
				done_on=?,
				exec_time=?
			WHERE id=?
		"""
		params=[is_success,datetime.now(),total_time,row_id]
		with self.lock:			
			try:				
				with self.sqlite_conn:
					cur=self.sqlite_conn.cursor()
					res=cur.execute(query,params)
					print res
					cur.close()
			except Exception as e:				
				print "Failed to insert into db ! | "+str(e)
				#raise e
	