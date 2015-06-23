# Author: sWaRtHi
# Creted: Wed, 11 February 2015
# Description: creates jobs for Ashoka pull 

import avk_mt_api
from datetime import datetime,timedelta
from helpers import jobs,util_fun
# import httplib2,json,urllib,xmltodict,re,time,os  
# AvkMT_Pull configuration
NO_DAYS_TO_PULL=3
NAME ="AvkMT_Pull"
PROVIDER_ID=66

def start_pull(process_id,response=None):
	"""
		Starting Point for pull
	"""
	print "Start pulling from cities."
	res_from_cities=avk_mt_api.get_from_cities(process_id=process_id)
	print "start_pulling_to_cities."
	avk_mt_api.get_to_cities(process_id=process_id,from_city_response=res_from_cities)
	print "Get city pairs to pull routes."
	response_city_pairs=avk_mt_api.get_city_pairs(process_id=process_id)
	print "Start pulling routes."
	start_pulling_routes(process_id=process_id,response=response_city_pairs)
	print "Get all route_code"
	response_route_codes=avk_mt_api.get_route_codes(process_id=process_id)
	print "Start pulling pickups"
	start_pulling_pickups(process_id=process_id,response=response_route_codes)
	# Start pulling dropoffs
	# start_pulling_dropoffs(process_id=process_id,response=response_route_codes)
	return True

def start_pulling_routes(process_id,response=None):
	# print "start pulling routes"
	manager=jobs.JobsManager()
	city_pairs=[(city_pair["from_city_id"],city_pair["from_city_name"],city_pair["to_city_id"],city_pair["to_city_name"],) for city_pair in response]

	from_date=datetime.now()+timedelta(days=0) 
	to_date=from_date+timedelta(days=NO_DAYS_TO_PULL)	
	dt=from_date
	route_list=[]
	waiter=jobs.JobsWaiter(manager,process_id)
	while dt<=to_date:
		for city_pair in city_pairs:
			route_list.append((city_pair[0],city_pair[1],city_pair[2],city_pair[3],dt.strftime("%Y-%m-%d")))
		dt=dt+timedelta(days=1)		

	for route in route_list:
		job=("avk.avk_mt_api",\
				"get_routes",{\
					"process_id":process_id,\
					"from_city_id":route[0],\
					"from_city_name":route[1],\
					"to_city_id":route[2],\
					"to_city_name":route[3],\
					"journey_date":route[4]\
				}\
			)
		# manager.add_job(job, callback_list=[waiter.get_callback_job()])
		waiter.add_job(job)
	waiter.wait(timeout=3*60*60)	
	del manager

def start_pulling_pickups(process_id,response):
	# print "start pulling routes"
	manager=jobs.JobsManager()
	if len(response)<=0:
		raise Exception("no result from get route codes")
	
	# job_list=set()
	waiter=jobs.JobsWaiter(manager,process_id)
	for route_code in response:
		job=("avk.avk_mt_api","get_pickups",{"process_id":process_id,"route_code":route_code["route_code"]})
		# manager.add_job(job,
			# callback_list=[waiter.get_callback_job()])
		waiter.add_job(job)

	waiter.wait(timeout=3*60*60)	
	del manager

def start_pulling_dropoffs(process_id,response):
	# print "start pulling routes"
	manager=jobs.JobsManager()
	if len(response)<=0:
		raise Exception("no result from get route codes")
	
	# job_list=set()
	waiter=jobs.JobsWaiter(manager,process_id)
	for route_code in response:
		job=("avk.avk_mt_api","get_dropoffs",{"process_id":process_id,"route_code":route_code["route_code"]})
		# manager.add_job(job,
			# callback_list=[waiter.get_callback_job()])				
		waiter.add_job(job)

	waiter.wait(timeout=3*60*60)	
	del manager

def process_data(process_id,response=None):
	"""
		End Point to process pulled data into gdsdb
	""" 
	avk_mt_api.process_data(process_id=process_id)

if __name__ == '__main__':
	# process_id = 53402637
	# response_city_pairs=avk_mt_api.get_city_pairs(process_id=process_id)
	# print response_city_pairs
	# print json.dumps(test_start_pulling_routes(process_id=process_id,response=response_city_pairs),indent=4)

	# response_route_codes=avk_mt_api.get_route_codes(process_id=process_id)
	# print response_route_codes
	# exit()
	# print json.dumps(start_pulling_pickups(process_id=process_id,response=response_route_codes),indent=4)
	# print json.dumps(start_pulling_pickups(process_id=process_id,response=response_route_codes),indent=4)
	# print json.dumps(start_pulling_dropoffs(process_id=process_id,response=response_route_codes),indent=4)

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

	print "Start Processing data for ProcessId : %d" % (process_id,)
	process_data(process_id=process_id)
	print "End Processing data for ProcessId : %d" % (process_id,)
	pass