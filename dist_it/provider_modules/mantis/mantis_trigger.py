# Author: Heera
# Creted: 2015-04-07

import mantis_api
from datetime import datetime,timedelta
from helpers import jobs,util_fun,gds_api
# import httplib2,json,urllib,xmltodict,re,time,os  
# import json

PROVIDER_ID=15
TOTAL_PULL_DAYS=45

def __do_nothing__(*args,**kwrds):
	pass
def __get_trip_journey_status__(process_id,trip_id,journey_date):	
	mantis_api.get_trip_journey_status(process_id,trip_id,journey_date)
	mantis_api.process_trip_journey_status(process_id)
	util_fun.refresh_routes(trip_id,PROVIDER_ID,journey_date,journey_date)

def __get_trip_journey__(process_id,trip_id,journey_date):
	mantis_api.get_trip_journey(process_id,trip_id,journey_date)
	mantis_api.process_trip_data(process_id)
	util_fun.refresh_routes(trip_id,PROVIDER_ID,journey_date,journey_date)

def __get_all_trip_journey__(process_id,trip_id):
	#mantis_api.get_scheduled_journey(trip_id)
	today=datetime.now()

	for i in range(TOTAL_PULL_DAYS):
		cur_date=today+timedelta(days=i)
		journey_date=cur_date.strftime("%Y-%m-%d")
		try:
			print "Process_Id=%d, Trip_id=%s, journey_date=%s" %(process_id,str(trip_id),str(journey_date))
			mantis_api.get_trip_journey(process_id,trip_id,journey_date)
		except Exception as e:
			pass
	end_date=today+timedelta(days=TOTAL_PULL_DAYS)
	mantis_api.process_trip_data(process_id)
	util_fun.refresh_routes(trip_id,PROVIDER_ID,today.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))

dict_handlers={}
dict_handlers["tripquota"] 		= __get_all_trip_journey__
dict_handlers["triparrgtran"] 	= __get_trip_journey__
dict_handlers["tripstopbkg"] 	= __get_trip_journey_status__

def handle_trigger(key,*args,**kwrds):
	"""
		call appropriate handler according to key
	"""
	if mantis_api.is_trigger_enabled()==False:
		raise Exception("Trigger is disabled")

	global dict_handlers
	process_id=util_fun.get_process_id(15)
	print "process_id=%d"% process_id
	#kwrds["process_id"]=process_id
	dict_args=kwrds["args"]	
	handler_args=[process_id]+dict_args
	#dict_args["process_id"]=process_id
	if key in dict_handlers:
		handler=dict_handlers[key]
		handler(*handler_args)
	else:
		raise Exception("No handler found for key= %s!" % key)
	


if __name__ == '__main__':		
	handle_trigger("tripstopbkg",args=["13021","2015-05-13"])
	#handle_trigger("tripquota",args={"trip_id":"13021"})
	#handle_trigger("tripquota",trip_id=23631)