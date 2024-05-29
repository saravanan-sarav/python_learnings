##########################################################################################################
#  Purpose  : MO Configuration File.
#             This is common config file that can be inherited by MO scripts to utilize the common     
#             variables, common functions, common path etc.
# Input file: It uses JSON file envconfig.json as a global environent input feed for MO config file.
##########################################################################################################
import json
import os
import cx_Oracle
import logging

logging.info("Derive Environment START")
logging.info("")

currentWorkingDirectory = os.getcwd()
logging.info("Current Working Directory is %s", currentWorkingDirectory)
logging.info("")

currentWorkingDirectoryParts = currentWorkingDirectory.split('/')
environment_name = currentWorkingDirectoryParts[3] 

logging.info("Environment is %s", environment_name)
logging.info("")

logging.info("Derive Environment END")
logging.info("")

#Function to load config from file
def load_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config_data = json.load(file)
        return config_data
    except IOError:
        print("JSON file not found OR error reading the file.")
    except json.JSONDecodeError:
        print("Error decoding JSON in config file.")





# Call function passing filename as parameter

config_file_path = '/shared/cibc/' + environment_name + '/batch/envconfig.json'
readconfig = load_config(config_file_path)

# Accessing values from the config
if readconfig:

		if environment_name == 'miswg':
			PYTHON_USER = readconfig.get('connstring', {}).get('username', '')
			PYTHON_PASSWORD = readconfig.get('connstring', {}).get('password', '')
			PYTHON_DSN = readconfig.get('connstring', {}).get('dsn', '')
			PYTHON_DEBUG_MODE = readconfig.get('debug_mode', False)

			MO_DATABASE = readconfig.get('MISWG', {}).get('modb', '')
			MO_DATABASE_WH = readconfig.get('MISWG', {}).get('mowh', '')
			db_mo=readconfig.get('MISWG', {}).get('modb', '')
			DSQUERY = readconfig.get('MISWG', {}).get('modb', '')

			branch_QR = readconfig.get('inputfilename', {}).get('branchQR', '')	
			representative_match = readconfig.get('inputfilename', {}).get('representativematch', '')
			ora_env_sp = readconfig.get('connstring', {}).get('env_sp', '')
			
		elif environment_name == 'miswguat1':

			PYTHON_USER = readconfig.get('connstring_uat1', {}).get('username', '')
			PYTHON_PASSWORD = readconfig.get('connstring_uat1', {}).get('password', '')
			PYTHON_DSN = readconfig.get('connstring_uat1', {}).get('dsn', '')
			PYTHON_DEBUG_MODE = readconfig.get('debug_mode', False)

			MO_DATABASE = readconfig.get('UAT1', {}).get('modb', '')
			MO_DATABASE_WH = readconfig.get('UAT1', {}).get('mowh', '')
			db_mo=readconfig.get('UAT1', {}).get('modb', '')
			DSQUERY = readconfig.get('UAT1', {}).get('modb', '')

			branch_QR = readconfig.get('inputfilename', {}).get('branchQR', '')	
			representative_match = readconfig.get('inputfilename', {}).get('representativematch', '')
			ora_env_sp = readconfig.get('connstring_uat1', {}).get('env_sp', '')

		elif environment_name == 'miswguat2':

			PYTHON_USER = readconfig.get('connstring_uat2', {}).get('username', '')
			PYTHON_PASSWORD = readconfig.get('connstring_uat2', {}).get('password', '')
			PYTHON_DSN = readconfig.get('connstring_uat2', {}).get('dsn', '')
			PYTHON_DEBUG_MODE = readconfig.get('debug_mode', False)

			MO_DATABASE = readconfig.get('UAT2', {}).get('modb', '')
			MO_DATABASE_WH = readconfig.get('UAT2', {}).get('mowh', '')
			db_mo=readconfig.get('UAT2', {}).get('modb', '')
			DSQUERY = readconfig.get('UAT2', {}).get('modb', '')

			branch_QR = readconfig.get('inputfilename', {}).get('branchQR', '')	
			representative_match = readconfig.get('inputfilename', {}).get('representativematch', '')
			ora_env_sp = readconfig.get('connstring_uat2', {}).get('env_sp', '')


	
logging.info("PYTHON_USER is %s", PYTHON_USER)
logging.info("PYTHON_DSN is %s", PYTHON_DSN)

	
CSVRPTDIR = os.getcwd()

ora_connection = cx_Oracle.connect(PYTHON_USER, PYTHON_PASSWORD, PYTHON_DSN)

cx_Oracle_cursor = cx_Oracle.CURSOR

ora_cursor = ora_connection.cursor()

sftp_incoming_path = '/shared/cibc/'+ environment_name + '/batch/sftp/incoming'



