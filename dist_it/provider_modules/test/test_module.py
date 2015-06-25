#Author: Heera
#Date: 2014-08-26
#Description: Provider function modules
from helpers import jobs
#print 2
import time
def squar_it(ip,response=None):
	
	time.sleep(10)	
	return ip*ip

def sum(*args):
	val=0
	for i in args:
		val+=i
	return val
def return_it(*args,**kwrds):
	return kwrds

def new_callback_test()	:
	print jobs.__file__
	manager=jobs.JobsManager()

	waiter=jobs.JobsWaiter(manager,"test3")	
	job=("test.test_module","return_it",{"b":1,"a":2,"c":3})

	#,[waiter.get_callback_job()]
	print jobs.__file__
	#jobs.add_job(job)
	waiter.add_job(job)
	job=("test.test_module","return_it",{"a":10,"b":20,"c":30})
	waiter.add_job(job)
	#print "waiting..."	
	waiter.wait(timeout=20)	
	print "Done"
	#time.sleep(50)
def add_trigger_test():	
	dist_it_auth_key= "60c05c632a2822a0a877c7e991602543"
	dist_it_port= 8004
	dist_it_ip= "127.0.0.1"
	jobs_pusher=jobs.Jobs_Pusher(dist_it_ip,dist_it_port,dist_it_auth_key)
	jobs_pusher.add_job(("mantis.mantis_trigger","handle_trigger",{"key":"test_trigger","trigger_time":"2015-05-12 13:47:00","args":[123]}))

if __name__=='__main__':
	#add_trigger_test()
	new_callback_test()