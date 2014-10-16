##Author: sWaRtHi
##Creted: 2014-10-16
##Description: creates jobs for TicketEngine pull 

NO_DAYS_TO_PULL=0
NAME ="TicketEngine_Pull"

import ticketengine_api
from datetime import datetime,timedelta
import time
from helpers import jobs

def start_pull(process_id,response=None):
	"""
		Starting Point for pull
	"""
	print "pulling cities"
	response = ticketengine_api.get_cities(process_id = process_id)
	print "pullling routes"
	response_city_pairs = ticketengine_api.get_city_pairs_to_pull(process_id=process_id)
	
	return start_pulling_routes(process_id=process_id,response=response_city_pairs)

def start_pulling_routes(process_id,response=None):
	print "start pulling routes"
	# manager=jobs.JobsManager()
	city_pairs=[(city_pair["from_city_id"],city_pair["to_city_id"]) for city_pair in response]
	
	from_date=datetime.now()+timedelta(days=0) 
	to_date=from_date+timedelta(days=NO_DAYS_TO_PULL)	
	dt=from_date
	route_list=[]
	# waiter=jobs.JobsWaiter(manager,process_id)
	while dt<=to_date:
		for city_pair in city_pairs:
			route_list.append((city_pair[0],city_pair[1],dt.strftime("%Y-%m-%d")))
		dt=dt+timedelta(days=1)		
	
	for route in route_list:
		job=("ticketengine.ticketengine_api","get_routes",{"process_id":process_id,"from_city_id":route[0],"to_city_id":route[1],"journey_date":route[2]})
		#manager.add_job(job,
			# callback_list=[waiter.get_callback_job()])				

	# 	waiter.add_job(job)

	# #wait_for_jobs(job_list,status_obj,key)			
	# waiter.wait(timeout=3*60*60)	
	# del manager

if __name__ == '__main__':
	pass
