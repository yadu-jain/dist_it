#Author: Heera
#Date: 2014-08-26
#Description: Provider function modules
from helpers import jobs
#print 2
import time
def squar_it(ip,response=None):
	
	time.sleep(10)	
	return ip*ip
	
def new_callback_test()	:
	print jobs.__file__
	manager=jobs.JobsManager()

	waiter=jobs.JobsWaiter(manager,"test3")	
	job=("test.test_module","squar_it",{"ip":1})
	#,[waiter.get_callback_job()]
	print jobs.__file__
	#jobs.add_job(job)
	waiter.add_job(job)
	#print "waiting..."	
	waiter.wait(timeout=20)	
	print "Done"
	#time.sleep(50)

if __name__=='__main__':
	new_callback_test()