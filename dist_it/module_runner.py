#Author: Heera
#Date: 2014-08-26
#Description: Library for puller modules
#from provider_modules import *
import sys
import time
import codecs
import traceback
class StreamDirecter(codecs.StreamWriter):
    encode = codecs.utf_8_encode    
    def __init__(self,fun,*args,**kw):        
        self.fun=fun
    def write(self,obj):
        try:            
            self.fun(str(obj))
        except:
            pass
def run(module_name,fun_name,args,log_fun):
	result=None
	success=False
	error=None
	stdout=sys.stdout
	try:
		module=None
		module_full_path=None
		if module_name.split(".")[0]=="helpers":
			module_full_path=module_name
		else:
			module_full_path="provider_modules." + module_name

		if sys.modules.has_key(module_full_path):
			module=sys.modules[module_full_path]
			
		else:
			module=__import__(module_full_path, fromlist=[''])	
		parent_module=module
		for attr in fun_name.split("."):
			fun = getattr(parent_module,attr)
			parent_module=fun				

		###		
		sys.stdout=StreamDirecter(log_fun)								
		result=fun(**args)
		sys.stdout=stdout
		###
		success=True
	except Exception as ex:
		print ex
		sys.stdout=stdout
		error="\t".join(traceback.format_exc().splitlines())
		success=False
	return {
			"success":success,
			"result":result,
			"error": error
			}
	#module.squar_it(2)
	#fun=getattr(module,"squar_it")
	#fun(2)
	#test_module.test()
#print run("test_module","squar_it",{"ip":2})
def fun_temp(data):
	pass
#print "running"
#run("parveen.parveen_api","test_me",{},fun_temp)
#run("parveen.parveen_pull","Waiter.callback",{},fun_temp)


