#Author: Heera
#Date: 2014-08-26
#Description: Manages the client and assign jobs in queue to processes 

import multiprocessing as mp
import Queue
from multiprocessing.managers import SyncManager
import module_runner
from datetime import datetime
import os
from multiprocessing import Value
import time
import socket
import json
from json import JSONEncoder
from helpers import server_config as config
import pprint
#AUTHKEY= "60c05c632a2822a0a877c7e991602543"
#PORTNUM = 8004 #Preffered port
#PRODUCER_IP='127.0.0.1'#"10.66.60.90"
pp = pprint.PrettyPrinter(indent=1)

CLIENT_NAME=socket.gethostname()
CONSUMER_NAME="MEEPO"

class JobsConsumer(SyncManager):
	pass

QUEUE_CHECK_TIMEOUT=5
SERVER_SYNC_INTERVAL=5

class MyEncoder(JSONEncoder):
    def default(self, o):
        return str(o)

def make_client_manager(ip, port, authkey):
	""" Create a manager for a client. This manager connects to a server on the
		given address and exposes the get_job_q and get_result_q methods for
		accessing the shared queues from the server.
		Return a manager object.
	"""

	JobsConsumer.register('get_job_q')
	JobsConsumer.register('get_result_q')
	JobsConsumer.register('get_logger_q')
	JobsConsumer.register('get_pool_config')



	manager = JobsConsumer(address=(ip, port), authkey=authkey)
	manager.connect()

	print 'Client connected to %s:%s' % (ip, port)
	return manager

def log_local(str_obj):
	"""
	Used only for testing writing logs localy
	"""
	with open ("log.txt","a") as f:
		f.write(str_obj)
		f.flush()

def execute_job(job_q, result_q,logger_q,status,flag_terminate):
	""" A worker function to be launched in a separate process. Takes jobs from
		job_q - each job a list of numbers to factorize. When the job is done,
		the result (dict mapping number -> list of factors) is placed into
		result_q. Runs until job_q is empty.
	"""
	print "pid ",os.getpid()," waiting for jobs...."
	while True:
		job=None
		try:
			time.sleep(0.01)
			if flag_terminate.value==1:
				status.value='t' #finished
				break
			status.value='w' #waiting
			job = job_q.get(True,timeout=QUEUE_CHECK_TIMEOUT)
			status.value='r' #running			
			pid=os.getpid()
			def log_fun(str_data):
				##!IMPTORTANT DO NOT PUT PRINT STATEMENT IN THIS FUN
				if str_data=='\n':
					return
				try:
					d={"pid":pid,"log":str_data,"job":job}				
					logger_q.put_nowait(d)
				except Exception as ex:					
					pass
			#print logger_q
			start_time=datetime.now()
			op_dict=module_runner.run(job[0],job[1],job[2],log_fun)
			end_time=datetime.now()
			total_time=(end_time-start_time).total_seconds()
			op_dict["request"]=job			
			op_dict["total_time"]=total_time
			result_q.put(op_dict)			
			pp.pprint([os.getpid(),op_dict["success"],op_dict["error"],job[:2]])
			#print "\n"
			#print os.getpid()," did job ",job, op_dict["success"],op_dict["error"]
		except Queue.Empty:			
			if flag_terminate.value==1:
				status.value='t' #finished
				print "breaking..."
				break		
			continue


def create_consumer_pool(manager,pool_size,client_logger_q):	
	shared_job_q=manager.get_job_q()     
	shared_result_q = manager.get_result_q()
	#client_logger_q = manager.get_logger_q()
	pool_metadata=[]
	procs = []
	for i in range(pool_size):
		status=Value('c','s') #Type=char, initial value='s' ##starting
		flag_terminate=Value('i',0) #Type=int, initial value=0 ##no
		p = mp.Process(
				name=CONSUMER_NAME+"_"+str(i),
			    target=execute_job,
			    args=(shared_job_q, shared_result_q,client_logger_q, status,flag_terminate))
				#args=(shared_job_q, status,flag_terminate))	
		procs.append(p)	
		pool_metadata.append({"status":status,"flag_terminate":flag_terminate,"proc":p}	)
	return procs,pool_metadata

def create_pool(name):
	server_config=config.Server_Config()
	AUTHKEY 	= server_config.get_config("authkey")
	PORTNUM 	= int(server_config.get_config("portnum"))
	PRODUCER_IP = server_config.get_config("ip")
	print "Creating pool ",name
	manager = make_client_manager(PRODUCER_IP, PORTNUM, AUTHKEY)	
	d=manager.get_pool_config(name)
	pool_size=d[name]["pool_size"]
	if pool_size==0:
		pool_size=mp.cpu_count()
		my_config=d[name]
		my_config["pool_size"]=pool_size
		d[name]=my_config

	client_logger_q=mp.Queue()
	procs, meta_data=create_consumer_pool(manager, pool_size,client_logger_q)   	
	return procs,meta_data,d,manager,client_logger_q

def run_pool(procs,meta_data):
	print "Running pool: \n",json.dumps(meta_data,cls=MyEncoder,indent=4)
	for p in procs:
		p.start()
		
def kill_pool(procs,meta_data):
	print "killing pool"
	try:
		for proc_metadata in meta_data:			
			proc_metadata["flag_terminate"].value=1		
			print proc_metadata
		print "Waiting for threshfold time to let processes to release shared resources"
		time.sleep(QUEUE_CHECK_TIMEOUT*2)
		print "killing Proceses Forcefully"
		for proc in procs:
			if proc.is_alive()==True:				
				proc.terminate()
		return True
	except Exception as ex:
		print ex
		return False

def write_logs(client_logger_q,shared_logger_q):
	while True:
		time.sleep(0.01)
		try:
			log=client_logger_q.get_nowait()
			#TODO: uncomment following
			#shared_logger_q.put_nowait(log)
		except Queue.Empty:			
			break
		except Exception as ex:
			print "error while flushing logs to server:",ex			
			break

if __name__ == '__main__':
	name=CLIENT_NAME
	print "CONSUMER_NAME: ",CLIENT_NAME
	flag_refresh=True
	pools_conf=None
	while True:
		if flag_refresh==True:
			print "Getting consumer configuration from producer server..."
			procs, meta_data,pools_conf,manager,client_logger_q=create_pool(name=name)
			print json.dumps(pools_conf,cls=MyEncoder,indent=4)
			shared_logger_q=manager.get_logger_q()
			run_pool(procs,meta_data)	
			flag_refresh=False
		else:
			time.sleep(SERVER_SYNC_INTERVAL)				
			# Check if any configuration change by server and flush logs to server
			try:
				cmd=pools_conf[name] 				
				write_logs(client_logger_q,shared_logger_q)
				if "restart" in cmd and cmd["restart"]==True:
					print "executing server command: ",cmd					
					del cmd["restart"]
					pools_conf[name]=cmd
					flag_refresh=True
					kill_success=kill_pool(procs,meta_data)
					
					# Clean resource
					del manager
					del client_logger_q
					del procs
					del meta_data
					if kill_success==False:
						break				
					else:
						print "killing pool succeed"

				if "shutdown" in cmd and cmd["shutdown"]==True:
					print cmd
					del cmd["shutdown"]
					pools_conf[name]=cmd
					kill_success=kill_pool(procs,meta_data)

					# Clean resource					
					del client_logger_q
					del procs
					del meta_data
					del shared_logger_q
					del manager
					break				
			except IOError:
				print "Connection Lost"				
				for p in procs:
					if p.is_alive()==True:
						continue

				# Clean resource				
				del client_logger_q
				del procs
				del meta_data
				del shared_logger_q
				del manager
				break
				#kill_pool(procs,meta_data)
				#print "Exiting program"				
				#break
	
	

