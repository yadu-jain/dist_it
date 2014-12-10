##Author: sWaRtHi
##Creted: 2014-10-16
##Description: creates jobs for TicketEngine pull 

import ticketengine_api
from datetime import datetime,timedelta
import time
from helpers import jobs,util_fun

##TicketEngine Pull configuration
NO_DAYS_TO_PULL=30
NAME ="TicketEngine_Pull"
PROVIDER_ID=62

def start_pull(process_id,response=None):
	"""
		Starting Point for pull
	"""
	print "pulling cities"
	ticketengine_api.get_cities(process_id = process_id)
	# print "pulling companies"
	# ticketengine_api.get_companies(process_id=process_id)
	print "pullling city pairs"
	ticketengine_api.get_city_pairs(process_id = process_id)
	print "pullling routes"
	print "Get city_pairs from db for process_id="+str(process_id)
	response_city_pairs = ticketengine_api.get_city_pairs_to_pull(process_id=process_id)
	return start_pulling_routes(process_id=process_id,response=response_city_pairs)

def start_pulling_routes(process_id,response=None):
	print "start pulling routes"
	manager=jobs.JobsManager()
	city_pairs=[(city_pair["from_city_id"],city_pair["to_city_id"]) for city_pair in response]
	
	from_date=datetime.now()+timedelta(days=0) 
	to_date=from_date+timedelta(days=NO_DAYS_TO_PULL)	
	dt=from_date
	route_list=[]
	waiter=jobs.JobsWaiter(manager,process_id)
	while dt<=to_date:
		for city_pair in city_pairs:
			route_list.append((city_pair[0],city_pair[1],dt.strftime("%Y-%m-%d")))
		dt=dt+timedelta(days=1)		
	
	for route in route_list:
		job=("ticketengine.ticketengine_api","get_routes",{"process_id":process_id,"from_city_id":route[0],"to_city_id":route[1],"journey_date":route[2]})
		#manager.add_job(job,
		#	callback_list=[waiter.get_callback_job()])				
		waiter.add_job(job)

	waiter.wait(timeout=3*60*60)	
	del manager

def process_data(process_id,response=None):
	"""
		End Point to process pulled data into gdsdb
	""" 
	ticketengine_api.process_data(process_id=process_id)

if __name__ == '__main__':
	print NAME
	print "PROVIDER_ID=%d" % (PROVIDER_ID,)

	print "Defining Pulling dates"
	jd_from=datetime.now()+timedelta(days=1)
	jd_to=jd_from+timedelta(days=NO_DAYS_TO_PULL-1)
	print "Pulling From=%s, To=%s" % (jd_from.strftime("%Y-%m-%d"),jd_to.strftime("%Y-%m-%d"))	
	
	print "Get ProcessId from GDS"
	process_id = util_fun.get_process_id(PROVIDER_ID)	
	print "Generated ProcessId : %d" % (process_id,)
	
	print "Start Pulling data for ProcessId : %d" % (process_id,)
	start_pull(process_id)
	print "End Pulling data for ProcessId : %d" % (process_id,)
	
	# process_id = 48775616
	print "Start Processing data for ProcessId : %d" % (process_id,)
	process_data(process_id=process_id)
	print "End Processing data for ProcessId : %d" % (process_id,)
	pass
