##Author: Heera
##Creted: 2014-09-14
##Description: creates jobs for Parveen pull 
from helpers import job
from datetime import datetime,timedelta

##Parveen pull configuration
NO_DAYS_TO_PULL=1
NAME ="PARVEEN_PULL"

def start_pulling_to_cities(process_id,response=None):	
	#print "starting pulling to cities"
	manager=job.ClientMananger()
	
	if response["success"]==False:
		raise Exception("no result from get stations")
	#from_city_list=[("bangalore",42)]
	
	from_city_list=[(station["stationName"],station["stationId"]) for station in response["result"]["data"]["stations"]]
	job_list=set()

	waiter=job.Waiter(manager,process_id)		
	for from_city in from_city_list:
		job=("parveen.parveen_api","get_to_cities",{"process_id":process_id,"from_city_name":from_city[0],"from_city_id":from_city[1]})
		manager.add_job(job,
			#callback_list=[])				
			callback_list=[waiter.get_callback_job()])				

		waiter.add_task(job)

	print "waiting for jobs to complete"
	#wait_for_jobs(job_list,status_obj,key)	
	waiter.wait(timeout=30*60)
	print "starting pullling routes"		
	manager.add_job(("parveen.parveen_api","get_city_pairs_to_pull",{"process_id":process_id}),[("parveen.parveen_pull","start_pulling_routes",{"process_id":process_id})])		
	print "to cities done"
	del manager


def start_pulling_routes(process_id,response=None):
	print "start pulling routes"
	manager=job.ClientMananger()
	city_pairs=[(city_pair["from_city_id"],city_pair["to_city_id"]) for city_pair in response["result"]]
	
	from_date=datetime.now()+timedelta(days=1) # "2014-09-03"#datetime.strptime("2014-08-30","%Y-%m-%d")
	to_date=from_date+timedelta(days=NO_DAYS_TO_PULL)	
	dt=from_date
	route_list=[]
	waiter=job.Waiter(manager,process_id)
	while dt<=to_date:
		for city_pair in city_pairs:
			route_list.append((city_pair[0],city_pair[1],dt.strftime("%Y-%m-%d")))
		dt=dt+timedelta(days=1)
		
	
	for route in route_list:
		job=("parveen.parveen_api","get_routes",{"process_id":process_id,"from_city_id":route[0],"to_city_id":route[1],"journey_date":route[2]})
		manager.add_job(job,
			callback_list=[waiter.get_callback_job()])				

		waiter.add_task(job)

	#wait_for_jobs(job_list,status_obj,key)			
	waiter.wait(timeout=3*60*60)
	
	manager.add_job(("parveen.parveen_pull","process_data",{"process_id":process_id}),[("parveen.parveen_pull","process_completed",{"process_id":process_id})])		
	print "Completed"
	del manager

def process_data(process_id,response=None):
	"""
		End Point to process pulled data into gdsdb
	""" 
	pass

def process_completed(process_id):
	print "processed"

def start_pull(process_id,response=None):
	"""
		Starting Point for pull
	"""
	manager=job.ClientMananger()
	
	manager.add_job(("parveen.parveen_api","get_stations",{"process_id":process_id}),
		callback_list=[("parveen.parveen_pull","start_pulling_to_cities",{"process_id":process_id})])	
	del manager


###-----------------------Testing apis-------------------------------------------------------------------####
def test_api(process_id):
	manager=job.ClientMananger()
	manager.add_job(("parveen.parveen_api","get_city_pairs_to_pull",{"process_id":process_id}),[("parveen.parveen_pull","start_pulling_routes",{"process_id":process_id})])		

def check_callbacks():
	"""
		Used only while testing
	"""
	manager=job.ClientMananger()
	print manager.get_sync_data()
	print manager.get_callbacks_dict()	

def new_callback_test()	:
	manager=job.ClientMananger()
	waiter=job.Waiter(manager,"test")
	manager.add_job(("test.test_module","squar_it",{"ip":1}),[waiter.get_callback_job()])		

def start_pulling_to_cities_test(process_id,response=None):	
	#print "starting pulling to cities"
	manager=job.ClientMananger()
	#status_obj=manager.get_sync_data()
	#key=str(process_id)	
	#if not key in  status_obj:
	#	status_obj[key]={}
	#if response["success"]==False:
	#	raise Exception("no result from get stations")
	from_city_list=[("bangalore",42),("chennai",43)]
	
	#from_city_list=[(station["stationName"],station["stationId"]) for station in response["result"]["data"]["stations"]]
	job_list=set()

	manager=job.ClientMananger()
	for from_city in from_city_list:
		job=("parveen.parveen_api","get_to_cities",{"process_id":process_id,"from_city_name":from_city[0],"from_city_id":from_city[1]})
		manager.add_job(job,
			#callback_list=[])				
			callback_list=[waiter.get_callback_job()])				
		waiter.add_task(job)
		#job_list.add(pickle.dumps(job))

	print "waiting for jobs to complete"
	#wait_for_jobs(job_list,status_obj,key)	
	waiter.wait(timeout=30*60)
	print "all_tasks="+ str(waiter.get_tasks())
	print "starting pullling routes"		
	#manager.add_job(("parveen.parveen_api","get_city_pairs_to_pull",{"process_id":process_id}),[("parveen.parveen_pull","start_pulling_routes",{"process_id":process_id})])		
	print "to cities done"
	del manager	
###------------------------------------------------------------------------------------------------------####



if __name__=='__main__':
	
	start_pull(44553225)
	#new_callback_test()
	##start_pulling_to_cities_test(43862472)
	#check_callbacks()
	#test_api(22)
