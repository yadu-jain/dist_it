#Author: Heera
#Date: 2014-08-26
#Description: Manages the server and puts jobs in queue
import multiprocessing as mp
from multiprocessing.managers import SyncManager,BaseManager, DictProxy
from UserString import MutableString
from datetime import datetime
import Queue
import json
import time
import os
import pickle
import sys, traceback

AUTHKEY= "60c05c632a2822a0a877c7e991602543"
PORTNUM = 8004 #Preffered port
IP="10.66.60.90" #'127.0.0.1'

LOG_FLUSH_TIMEOUT=60*5 # Seconds
LOG_BUFFER_SIZE=10
DEFAULT_POOL_CONFIG={"pool_size":0}
LOG_DIR="D:\Pull_logs"
class JobsManager(SyncManager):
	pass


def callback_handler(callbacks,shared_result_q,shared_logger_q):
	print "recieving callbacks"
	while True:
		try:
			response = shared_result_q.get()
			default_callback(response,shared_logger_q)		
			callbacks.process_callback(response)									
			time.sleep(0.01)
		except Exception as e:			
			print "callback_handler:",e
			traceback.print_exc()
			break

def default_callback(response,shared_logger_q):
	if response["success"]==False:		
		shared_logger_q.put_nowait(response)


def test_log(obj)	:
	with open("test_log.txt","a") as f:
		f.write(str(obj))
		f.flush()

class CallBacks(object):

	def __init__(self,shared_job_q,callbacks_dict):
		self.callbacks_dict=callbacks_dict
		self.shared_job_q=shared_job_q
	
	def process_callback(self,response):
		request = response["request"]				
		if "response" in request[2]:
			del request[2]["response"]
		#test_log(request)
		str_req = pickle.dumps(request)
		if str_req in self.callbacks_dict:
			callback = self.callbacks_dict[str_req]						
			del self.callbacks_dict[str_req]
			#if response["success"]==True:			
						
			callback[2]["response"]=response
			self.shared_job_q.put(callback)				
	
class ClientProxies:

	def __init__(self):
		self.job_q = mp.Queue()
		self.result_q = mp.Queue()
		self.logger_q = mp.Queue()
		self.pools={}
		self.callbacks_dict ={}
		self.sync_data={}
		self.waiters={}
#		self.manager=None

	def __get_default_pool_config__(self):
		return {"pool_size":10}

	def get_job_q(self):
		return self.job_q

	def get_result_q(self):
		return self.result_q

	def get_logger_q(self):
		return self.logger_q
	def get_sync_data(self):
		return self.sync_data

	def get_pool_config(self,id):	
		#return self.manager==None		
		if not id in self.pools:
			print "creating new client pool "+str(id)
			self.pools[id]=DEFAULT_POOL_CONFIG
		return self.pools

	def add_job(self,job,callback_list=[]):		
		if type(job)==tuple:
			if len(job)==2:
				job.add({})			
			if len(job)==0:
				raise Exception("Invalid job !")
		else:
			raise Exception("Invalid job !")
		self.job_q.put(job)				
		req=job		
		for callback_job in  callback_list:
			if type(req)==tuple:				
				if len(req)==2:
					req.add({})			
				if len(callback_job)==2:
					callback_job.add({})			
				if len(callback_job)==0:
					raise Exception("Invalid callbacks !")
				else:
					self.callbacks_dict[pickle.dumps(req)]=callback_job
					req=callback_job
			else:
				raise Exception("Invalid callbacks !")


	def get_callbacks_dict(self):
		return self.callbacks_dict

	def get_waiter(self,name):
		#try:
		if name in self.waiters:
			return self.waiters[name]
		else:
			self.waiters[name]=mp.Queue()
			return self.waiters[name]

	def get_wait_list(self,name):
		#try:
		if name in self.waiters:
			return self.waiters[name]
		else:
			self.waiters[name]={}
			return self.waiters[name]
		#except Exception as e:
		#	raise Exception("get_waiter"+str(e))

	def delete_waiter(self,name):
		#try:
		if name in self.waiters:
			q=self.waiters[name]
			del self.waiters[name]
			del q
			return True
		else:
			return False
		#except Exception as e:
		#	raise Exception("get_waiter"+str(e))

		# print id
		# if id in self.pools:
		# 	return self.pools[id]
		# else:
		# 	return self.__get_default_pool_config__()
		#return self.manager.dict()
		#return self.result_q

	
	

def make_simple_server_manager(ip,port, authkey):
	print "starting server..."
	manager = BaseManager(address=('', 50000), authkey='abc')
	server = manager.get_server()
	server.serve_forever()
	print "started"
	return manager
	

def make_server_manager(ip,port, authkey):
	""" Create a manager for the server, listening on the given port.
	    Return a manager object with get_job_q and get_result_q methods.
	"""
	print "registering..."
	manager = JobsManager(address=(ip, port), authkey=authkey)			
	proxies=ClientProxies()	
	#proxies.manager=manager
	
	JobsManager.register('get_job_q', callable=proxies.get_job_q)
	JobsManager.register('get_result_q',  callable=proxies.get_result_q)
	JobsManager.register('get_logger_q', callable=proxies.get_logger_q)	
	JobsManager.register('get_sync_data',callable=proxies.get_sync_data,proxytype=DictProxy)
	JobsManager.register('get_pool_config',callable=proxies.get_pool_config,proxytype=DictProxy)
	JobsManager.register('get_callbacks_dict',callable=proxies.get_callbacks_dict,proxytype=DictProxy)
	JobsManager.register('get_waiter',callable=proxies.get_waiter)
	JobsManager.register('get_wait_list',callable=proxies.get_wait_list,proxytype=DictProxy)
	
	JobsManager.register('delete_waiter',callable=proxies.delete_waiter)	
	JobsManager.register('add_job',callable=proxies.add_job)
					
	print "Starting server ..."
	manager.start()	
	print 'Server started at port %s' % port
	return manager

def flush_log(str_data,file_name):
	"""
	Flush clients logs to disk
	"""
	path=os.path.join(LOG_DIR,file_name)	
	with open(path,"a") as f:
		f.write(str_data)
		f.flush()	


class JobsProducer(object):
	"""
	Used to add jobs, run server, shutdown server 
	"""
	ready=False
	status="starting"
	def __init__(self):
		pass		
	def add_job(self,job,callback_list=[]):
		if self.ready==False:
			raise Exception("Manager not ready yet")
		else:
			# self.shared_job_q.put(job)			
			# req=job
			# for callback_job in  callback_list:
			# 	self.shared_callbacks_dict[pickle.dumps(req)]=callback_job
			# 	req=callback_job
			self.manager.add_job(job,callback_list)

	def run(self):
		manager = make_server_manager(IP,PORTNUM, AUTHKEY)
		
		self.manager=manager
		self.shared_job_q=manager.get_job_q()    
		self.shared_result_q = manager.get_result_q()	
		self.shared_logger_q = manager.get_logger_q()
		self.shared_callbacks_dict = manager.get_callbacks_dict()

		##--------------------------------------------------------------##
		print "starting logs sink"
		self.logs_sink_process = mp.Process(
					target=logs_sink,
					args=(self.shared_logger_q,))
					#args=(shared_job_q, status,flag_terminate))		
		self.logs_sink_process.start()
		##--------------------------------------------------------------##

		##--------------------------------------------------------------##
		print "starting callback reciever"
		
		self.callbacks=CallBacks(self.shared_job_q, self.shared_callbacks_dict)
		

		self.callback_handler_process = mp.Process(
					target=callback_handler,
					args=(self.callbacks,self.shared_result_q,self.shared_logger_q))
		self.callback_handler_process.start()
		##--------------------------------------------------------------##

		self.ready=True
		self.status="started"
		print "server is ready"

	def shutdown(self):
		if self.status!="started":
			return
		self.status="shutting down"

		pool_config=self.manager.get_pool_config("LOCAL")
		d=pool_config["LOCAL"]
		d["shutdown"]=True
		pool_config["LOCAL"]=d

		seconds_to_shutdown=3
		for i in range(seconds_to_shutdown):
			print "shutting down in ",i+1," seconds ..."	
			time.sleep(1)    	

		self.manager.shutdown()   
		del self.manager
		print "server shutdown"
		print self.logs_sink_process.pid
		self.logs_sink_process.terminate()		
		print self.callback_handler_process.pid
		self.callback_handler_process.terminate()
		self.status="shut down"


def logs_sink(shared_logger_q):
	"""
	Function used to recieve logs from clients, this function is assigned to log sink process
	"""
	print "started logs sink"
	#print sys.executable
	logs_buffer=MutableString()
	curr_buff=0
	last_flushed=datetime.now()	
	#flush_log("test","test_log.txt")	
	flag=True
	while flag==True:
		try:
			log=shared_logger_q.get_nowait()			
			try:	
				print log			
				log_time=datetime.now()
				str_log=json.dumps(log)
				str_log=str_log.replace("\n","")
				str_log=str_log.replace("\r","")			
				str_log=log_time.strftime("%Y-%m-%d %H:%M:%S")+" "+str_log
				logs_buffer+="\n"+str_log
				diff=log_time-last_flushed
				curr_buff+=1
				if curr_buff >= LOG_BUFFER_SIZE or diff.total_seconds() > LOG_FLUSH_TIMEOUT:
					file_name="log_"+log_time.strftime("%Y_%m_%d_%H_%M") +".txt"
					last_flushed=log_time
					#flush_log(str(logs_buffer),file_name)
					last_flushed=datetime.now()
					curr_buff=0
				time.sleep(0.01)
			except Exception as e:
				print e
			#break
		except Queue.Empty:
			time.sleep(1)
		except:
			flag=False
			break


if __name__ == '__main__':	
	producer = JobsProducer()
	producer.run()
	#producer.add_job(("test_module","squar_it",{"ip":2}),[("test_module","squar_it",{"ip":10}),("test_module","squar_it",{"ip":12}),("test_module","squar_it",{"ip":13})])
	#producer.add_job(("test_module","squar_it",{"ip":3}),[("test_module","squar_it",{"ip":13})])
 	#producer.add_job(("test_module","squar_it",{"ip":2}))	
 	#producer.add_job(("test_module","squar_it",{"ip":3}))	
 	#producer.add_job(("test_module","squar_it",{"ip":4}))	
 	print "Default Python Interpreter: ", sys.executable
 	time.sleep(1)
 	print "-"*60
 	print "\n"
 	while True: 		
 		print "jobs=%d\tcallbacks=%d" % (producer.shared_job_q.qsize(),len(producer.shared_callbacks_dict))
		#TODO: Command line changes
		time.sleep(1) 		
	producer.shutdown()



