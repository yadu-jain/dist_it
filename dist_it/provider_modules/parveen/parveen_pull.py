##Author: Heera
##Creted: 2014-09-14
##Description: creates jobs for Parveen pull 
from helpers import jobs,util_fun
from datetime import datetime,timedelta
import time
import parveen_api


##Parveen pull configuration

NAME ="PARVEEN_PULL"
PROVIDER_ID=56

PRIMARY_BULK_SIZE=15
SECONDARY_BULK_SIZE=15
SECONDARY_BULK_COUNT=2
def start_pull(process_id,jd_from,jd_to):
	"""
		Starting Point for pull
	"""
	response = parveen_api.get_stations(process_id = process_id)	
	print "pulling city pairs"
	start_pulling_to_cities(process_id = process_id,response = response)
	print "pullling routes"		
	response_city_pairs = parveen_api.get_city_pairs_to_pull(process_id=process_id)	
	start_pulling_routes(process_id=process_id,
		jd_from=jd_from,
		jd_to=jd_to,
		response=response_city_pairs)	

def start_pulling_to_cities(process_id,response=None):		

	manager=jobs.JobsManager()
	
	if response["success"]==False:
		raise Exception("no result from get stations")
	#from_city_list=[("bangalore",42)]
	
	from_city_list=[(station["stationName"],station["stationId"]) for station in response["data"]["stations"]]
	job_list=set()

	waiter=jobs.JobsWaiter(manager,process_id)		
	for from_city in from_city_list:
		job=("parveen.parveen_api","get_to_cities",{"process_id":process_id,"from_city_name":from_city[0],"from_city_id":from_city[1]})		
		waiter.add_job(job)

	print "waiting for jobs to complete"	
	waiter.wait(timeout=30*60)
	print "to cities done"
	del manager


def start_pulling_routes(process_id,jd_from,jd_to,response=None):
	print "start pulling routes"
	manager=jobs.JobsManager()
	city_pairs=[(city_pair["from_city_id"],city_pair["to_city_id"]) for city_pair in response]		
	dt=jd_from
	route_list=[]
	waiter=jobs.JobsWaiter(manager,process_id)
	while dt<=jd_to:
		for city_pair in city_pairs:
			route_list.append((city_pair[0],city_pair[1],dt.strftime("%Y-%m-%d")))
		dt=dt+timedelta(days=1)		
	
	for route in route_list:
		job=("parveen.parveen_api","get_routes",{"process_id":process_id,"from_city_id":route[0],"to_city_id":route[1],"journey_date":route[2]})	
		waiter.add_job(job)
	waiter.wait(timeout=3*60*60)	## Max 3 hours 
	del manager

def process_data(process_id,response=None):
	"""
		End Point to process pulled data into gdsdb
	""" 
	parveen_api.process_data(process_id=process_id)




###-----------------------Testing apis Debug-------------------------------------------------------------------####

def fun_shyam(ram_name):
	manager=jobs.JobsManager()
	waiter=jobs.JobsWaiter(manager,ram_name)
	waiter.wait(10*60)
	manager.add_job(("test.test_module","squar_it",{"ip":2}),[])
	manager.add_job(("test.test_module","squar_it",{"ip":4}),[])


def fun_ram():
	manager=jobs.JobsManager()
	waiter=jobs.JobsWaiter(manager,"ram")	
	time.sleep(30)
	manager.add_job(("test.test_module","squar_it",{"ip":3}),[])			
	
def test_ram_shyam():
	manager=jobs.JobsManager()
	waiter =jobs.JobsWaiter(manager,"ram")	
	job=("parveen.parveen_pull","fun_ram",{})
	#manager.add_job(job,[])			
	waiter.add_job(job)
	manager.add_job(("parveen.parveen_pull","fun_shyam",{"ram_name":"ram"}),[])			
	#manager.add_job(("test.test_module","squar_it",{"ip":1}),[waiter.get_callback_job()])			


###------------------------------------------------------------------------------------------------------####


if __name__=='__main__':
	print NAME
	print "PROVIDER_ID=%d" % (PROVIDER_ID,)

	jd_from=datetime.now()+timedelta(days=1)
	jd_to=jd_from+timedelta(days=PRIMARY_BULK_SIZE-1)	
	print "Phase-1: From=%s, To=%s" % (jd_from.strftime("%Y-%m-%d"),jd_to.strftime("%Y-%m-%d"))	
	process_id = util_fun.get_process_id(PROVIDER_ID)	
	print "PROCESS_ID=%d" % (process_id,)
	start_pull(process_id,jd_from,jd_to)	
	process_data(process_id=process_id)	

	curr_secondary_counter=datetime.now().day % SECONDARY_BULK_COUNT
	jd_from=jd_to+timedelta(days=1+SECONDARY_BULK_SIZE*curr_secondary_counter)
	jd_to=jd_from+timedelta(days=SECONDARY_BULK_SIZE-1)	
	print "Phase-2: From=%s, To=%s" % (jd_from.strftime("%Y-%m-%d"),jd_to.strftime("%Y-%m-%d"))	
	process_id = util_fun.get_process_id(PROVIDER_ID)	
	print "PROCESS_ID=%d" % (process_id,)
	start_pull(process_id,jd_from,jd_to)	
	process_data(process_id=process_id)	