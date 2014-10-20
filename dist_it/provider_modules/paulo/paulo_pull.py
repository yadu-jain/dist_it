#Author: Heera
#Date: 2014-09-01
#Description: Manages the client and processes the jobs in queue

from helpers import jobs,util_fun
from datetime import datetime,timedelta
import paulo_api

##Parveen pull configuration
NO_DAYS_TO_PULL=1

NAME ="PAULO_PULL"
PROVIDER_ID=57

def start_pull(process_id):
	"""
		Starting point for pull
	"""	
	print "pulling cities"
	response_cities = paulo_api.get_cities(process_id=process_id)
	print "pulling city pairs"
	response_citypairs = paulo_api.get_city_pairs(process_id=process_id)
	response_citypairs_db = paulo_api.get_city_pairs_to_pull(process_id=process_id)
	print "pulling routes"
	response_routes = start_pulling_routes(process_id=process_id,response=response_citypairs_db)
	


def start_pulling_routes(process_id,response=None):
	print "start pulling routes"
	manager=jobs.JobsManager()
	city_pairs=[(city_pair["from_city_id"],city_pair["to_city_id"]) for city_pair in response]
	
	from_date=datetime.now()+timedelta(days=1) # "2014-09-03"#datetime.strptime("2014-08-30","%Y-%m-%d")
	to_date=from_date+timedelta(days=NO_DAYS_TO_PULL-1)	
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
def process_data(process_id):
	"""
		End Point to process pulled data into gdsdb
	""" 
	print "starting processing all data into db"
	paulo_api.process_data(process_id=process_id)


if __name__=='__main__':
	print NAME
	print "PROVIDER_ID=%d" % (PROVIDER_ID,)
	process_id = util_fun.get_process_id(PROVIDER_ID)	
	print "PROCESS_ID=%d" % (process_id,)
	#start_pull(process_id)	
	#process_data(process_id=46252770)	

	##----------Debug------------------------------------------###
	#print paulo_api.get_routes(12345,"WAI","VPI","2014-10-21")
