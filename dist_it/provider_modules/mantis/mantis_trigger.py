# Author: Heera
# Creted: 2015-04-07

import mantis_api
from datetime import datetime,timedelta
from helpers import jobs,util_fun,gds_api
# import httplib2,json,urllib,xmltodict,re,time,os  
# import json

PROVIDER_ID=15
TOTAL_PULL_DAYS=45

dict_handlers={}

def __do_nothing__(*args,**kwrds):
	pass
def handle_trip_status_changed(process_id,trip_id,journey_date=None):	
	"""
		1.get trip's status for all future dates in crs.
		2.update the status of trip journey in gds
		3.refresh trip journey status
	"""	
	min_date,max_date=mantis_api.get_trip_journey_status(process_id,trip_id,None)
	mantis_api.process_trip_journey_status(process_id)
	util_fun.refresh_routes(trip_id,PROVIDER_ID,min_date.strftime("%Y-%m-%d"),max_date.strftime("%Y-%m-%d"))

def handle_trip_journey_status_changed(process_id,trip_id,journey_date):	
	"""
		1.get trip's status for given journey date in crs.
		2.update the status of trip journey in gds
		3.refresh trip journey status
	"""	
	min_date,max_date=mantis_api.get_trip_journey_status(process_id,trip_id,journey_date)
	mantis_api.process_trip_journey_status(process_id)
	util_fun.refresh_routes(trip_id,PROVIDER_ID,min_date.strftime("%Y-%m-%d"),max_date.strftime("%Y-%m-%d"))

def __get_trip_journey__(process_id,trip_id,journey_date):
	"""
		1.get trip's routes for given journey date from crs
		2.process into gds
		3.refresh search cache of gds and ty
	"""
	mantis_api.get_trip_journey(process_id,trip_id,journey_date)
	mantis_api.process_trip_data(process_id)
	util_fun.refresh_routes(trip_id,PROVIDER_ID,journey_date,journey_date)

def handle_trip_scheduled(process_id,from_jd,to_jd,flag_dep,flag_coach,trip_id):
	"""
		1.Get trip's all details for given from_jd to to_jd
		2.process into gds
		3.refresh route details cache if flag_dep or flag_coach is 1
		4.refresh chart layout if flag_coach is 1
	"""
	from_date=datetime.strptime(from_jd,"%Y-%m-%d")
	end_date=datetime.strptime(to_jd,"%Y-%m-%d")
	end_date_plus1=end_date+timedelta(days=1)

	cur_date=from_date
	while cur_date < end_date_plus1 :		
		journey_date=cur_date.strftime("%Y-%m-%d")
		try:
			print "Process_Id=%d, Trip_id=%s, journey_date=%s" %(process_id,str(trip_id),str(journey_date))
			mantis_api.get_trip_journey(process_id,trip_id,journey_date)
		except Exception as e:
			pass
		cur_date=cur_date+timedelta(days=1)
	#end_date=today+timedelta(days=TOTAL_PULL_DAYS)
	mantis_api.process_trip_data(process_id)
	print flag_coach,flag_dep
	flag_coach=int(flag_coach)
	flag_dep=int(flag_dep)
	print flag_coach,flag_dep
	if flag_coach or flag_dep:		
		util_fun.refresh_routes_details(trip_id,PROVIDER_ID,None)
	if flag_coach:
		util_fun.refresh_layouts(trip_id,PROVIDER_ID,from_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
	util_fun.refresh_routes(trip_id,PROVIDER_ID,from_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))


def handle_trip_quota_updated(process_id,trip_id):
	"""
		1. get max scheduled date of the trip from crs
		2. get all trip journey routes from crs for all future journey date upto max scheduled date
		3. process into gds
		3.refresh search cache of gds and ty
	"""
	today=datetime.now()
	temp_response = mantis_api.get_max_trip_journey_date(trip_id)
	print temp_response
	str_end_date=None
	if len(temp_response)>0 and len(temp_response[0])>0 :
		str_end_date=temp_response[0][0]["JourneyDate"]

	if str_end_date == None:
		raise Exception("No Future scheduled date found !")

	cur_date=today
	end_date=datetime.strptime(str_end_date,"%Y-%m-%d")
	end_date_plus1=end_date+timedelta(days=1)
	while cur_date < end_date_plus1 :		
		journey_date=cur_date.strftime("%Y-%m-%d")
		try:
			print "Process_Id=%d, Trip_id=%s, journey_date=%s" %(process_id,str(trip_id),str(journey_date))
			mantis_api.get_trip_journey(process_id,trip_id,journey_date)
		except Exception as e:
			pass
		cur_date=cur_date+timedelta(days=1)
	#end_date=today+timedelta(days=TOTAL_PULL_DAYS)
	mantis_api.process_trip_data(process_id)
	print util_fun.refresh_routes(trip_id,PROVIDER_ID,today.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))	

def __get_trip_journey_pickups__(process_id,trip_id,journey_date):
	"""
		1.get latest trip journey routes from crs and refresh cache
		2. refresh its routes pickups timing
	"""	
	mantis_api.get_trip_journey(process_id,trip_id,journey_date)
	mantis_api.process_trip_data(process_id)
	util_fun.refresh_routes_details(trip_id,PROVIDER_ID,journey_date)
	util_fun.refresh_trip_pickups(trip_id,PROVIDER_ID,journey_date)

def handle_trip_pickups_changed(process_id,trip_id):
	"""
		refresh pickups of trip's all future routes
	"""
	util_fun.refresh_trip_pickups(trip_id,PROVIDER_ID,None)

def handle_pickup_chagned(process_id,pickup_id):
	"""
		1.get pickup details from crs 
		2.process into gds
		3.refresh pickup cache
	"""
	mantis_api.get_pickup_details(process_id,pickup_id)
	mantis_api.process_pickup_details(process_id)
	print util_fun.refresh_pickup_details(pickup_id,PROVIDER_ID)

##-------------------------------------Key Handler Map------------------------------------------------------------------------------## 
dict_handlers={
	"tripquota" 			: handle_trip_quota_updated,
	"tripstopbkg" 			: handle_trip_status_changed,
	"tripBlocked" 			: handle_trip_journey_status_changed,
	"tripUnblocked" 		: handle_trip_journey_status_changed,
	"servicepkptimechng" 	: handle_trip_pickups_changed,
	"pickupdtlchng" 		: handle_pickup_chagned,
	"tripscheduled" 		: handle_trip_scheduled,
}
##---------------------------------------------------------------------------------------------------------------------------------##



##--------------------------------------------Handler Demux------------------------------------------------------------------------##
def handle_trigger(key,*args,**kwrds):
	"""
		call appropriate handler according to key
	"""
	if mantis_api.is_trigger_enabled()==False:
		raise Exception("Trigger is disabled")

	global dict_handlers
	process_id=util_fun.get_process_id(15)
	print "process_id=%d"% process_id	
	dict_args=kwrds["args"]	
	handler_args=[process_id]+dict_args	
	if key in dict_handlers:
		handler=dict_handlers[key]
		#print handler
		handler(*handler_args)
	else:
		raise Exception("No handler found for key= %s!" % key)
	
if __name__ == '__main__':		
	#handle_trigger("tripBlocked",args=["2563","2015-05-17"])
	#handle_trigger("tripUnblocked",args=["2563","2015-05-18"])
	#handle_trigger("tripquota",args=["16943"])
	#handle_trigger("tripquota",trip_id=23631)
	#handle_trigger("pickupdtlchng",args=["32204"])
	#handle_trigger("depchng",args=["16943","2015-05-20"])
	#handle_trigger("tripscheduled",args=["2015-05-16","2015-05-16","1","1","16943"])
	handle_trigger("tripstopbkg",args=["16943"])
	#tripscheduled()
	pass