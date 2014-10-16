#Author: Heera
#Date: 2014-09-01
#Description: Manages the client and processes the jobs in queue

from helpers import jobs
from datetime import datetime,timedelta
import paulo_api

##Parveen pull configuration
NO_DAYS_TO_PULL=0

NAME ="PAULO_PULL"

def start_pull(process_id,response=None):
	manager=jobs.JobsManager()
	
	waiter=jobs.JobsWaiter(manager,process_id)		
	job=("paulo.paulo_api","get_city_pairs",{"process_id":process_id})	

	waiter.add_job(job)	
	waiter.wait(timeout=30*60)
	manager.add_job(("paulo.paulo_api","get_city_pairs_to_pull",{"process_id":process_id}),
		callback_list=[("paulo.paulo_pull","start_pulling_routes",{"process_id":process_id})])
	del manager


def start_pulling_routes(process_id,response=None):
	print "start pulling routes"
	manager=jobs.JobsManager()
	city_pairs=[(city_pair["from_city_id"],city_pair["to_city_id"]) for city_pair in response["result"]]
	
	from_date=datetime.now()+timedelta(days=1) # "2014-09-03"#datetime.strptime("2014-08-30","%Y-%m-%d")
	to_date=from_date+timedelta(days=NO_DAYS_TO_PULL)	
	dt=from_date
	route_list=[]
	waiter=jobs.JobsWaiter(manager,process_id)
	while dt<=to_date:
		for city_pair in city_pairs:
			route_list.append((city_pair[0],city_pair[1],dt.strftime("%Y-%m-%d")))
		dt=dt+timedelta(days=1)
		
	
	for route in route_list:
		job=("paulo.paulo_api","get_routes",{"process_id":process_id,"from_city_id":route[0],"to_city_id":route[1],"journey_date":route[2]})		

		waiter.add_job(job)
	
	waiter.wait(timeout=3*60*60)
	
	print "Completed"
	del manager

def process_data(process_id,response=None):
	print "processing data"
	return True

def process_completed(process_id):
	print "processed"

def test_api(process_id):
	manager=jobs.JobsManager()
	manager.add_job(("paulo.paulo_api","get_city_pairs_to_pull",{"process_id":process_id}),
		callback_list=[("paulo.paulo_pull","start_pulling_routes",{"process_id":process_id})])
def test1():
	print paulo_api.get_city_pairs_to_pull(1)

def check_callbacks():
	manager=jobs.JobsManager()
	print manager.get_sync_data()
	print manager.get_callbacks_dict()	

if __name__=='__main__':
	
	#start_pull(44468417)
	#start_pull(44474813)
	#start_pull(44483005)
	#start_pull(1)
	#test_api(1)
	test1()

	pass
		
	#start_pulling_to_cities_test(43862472)
	#check_callbacks()
	#test_api(22)
