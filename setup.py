from setuptools import setup, find_packages
setup(
    name='dist_it',
    version='0.1',
    description='install dist_it',
    author='Heera',
    include_package_data = True,
#    package_data={"": ["*.ini"]},
    install_requires=[
    				"pymssql"			## microsoft sql server driver
    				,"pip"				## installer
    				,"httplib2"			## http library
    				,"xmltodict"    	## XML to dictionary
    				,"docutils"
    				,"suds" 				## Soap Client
                    ,"pytz"             ## Time Zoned Date
    			],
    #setup_requires=["virtualenv"],
    packages = find_packages()
    )
