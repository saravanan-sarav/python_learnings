import logging
import sys
import os
import csv
import subprocess

"""
Created By:     Umesh Bhavsar 
Created Date:   02/12/2024
Purpose:        Central repository for common helper functions  
                which can be used across all scripts.           
"""

#logging.basicConfig(filename="helperFunctions.log", level=logging.DEBUG, filemode="w", format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logging.basicConfig(filename="helperFunctions.log", level=logging.DEBUG, filemode="w", format="%(message)s" )

logging.info("====================================================================================================================")
logging.info("")

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

logging.info("Importing moconfig file from batch directory - START")
logging.info("")

try:
    sys.path.insert(1, '/shared/cibc/' + environment_name + '/batch')

    logging.info("Batch directory path inserted")
    logging.info("")

    import moconfig

    logging.info("Importing moconfig file from batch directory - END")

    logging.info("")
except:
    logging.info("An error occurred while importing moconfig.py")
    logging.info("")
    exit()

def dropTempTables(tblName):
    logging.info("Checking if Table %s is already created", tblName)
    logging.info("")

    tblExists = ''

    rows = moconfig.ora_cursor.execute("select table_name from user_tables where table_name = '" + tblName.upper() + "'")

    for row in rows:
        tblExists = row[0]

    if tblExists:
        logging.info("Table %s is already created. Dropping it off", tblName)
        logging.info("")
        moconfig.ora_cursor.execute("drop table " + tblName)
        moconfig.ora_connection.commit()
        logging.info("Table %s dropped", tblName)
        logging.info("")
    else:
        logging.info("Table %s don't exists.", tblName)
        logging.info("")
        

def dropCreateTempTables(tblName, tblSchema):
    logging.info("Checking if Table %s is already created", tblName)
    logging.info("")

    moconfig.ora_cursor.execute("select table_name from user_tables where table_name = '" + tblName.upper() + "'")
   
    if moconfig.ora_cursor.fetchone() != None:
        logging.info("Table %s is already created. Dropping it off", tblName)
        logging.info("")
        moconfig.ora_cursor.execute("drop table " + tblName)
        moconfig.ora_connection.commit()
        logging.info("Table %s dropped", tblName)
        logging.info("")

    logging.info("Creating table %s", tblName)
    logging.info("")

    moconfig.ora_cursor.execute(tblSchema)
    moconfig.ora_connection.commit()

    logging.info("Table %s created", tblName)
    logging.info("")

def createIndexOnTable(tblName, columnName, indexName):
    logging.info("Creating index on %s column in %s table.", columnName, tblName)
    logging.info("")

    moconfig.ora_cursor.execute("create index " + indexName + " on " + tblName + "(" + columnName + ")")
    moconfig.ora_connection.commit()

    logging.info("Index %s created successfully on %s column in %s table.", indexName, columnName, tblName)
    logging.info("")

def checkAndCreateDirectory(dirToCheckAndCreate, path):
    logging.info("--------------------------------------------------------------------------------------")
    logging.info("")

    logging.info("Check if %s Directory exists.", dirToCheckAndCreate)
    logging.info("")

    report_directory = os.path.join(path, dirToCheckAndCreate)

    file_exists = os.path.exists(report_directory)

    if file_exists:
        logging.info("Directory %s exists.", dirToCheckAndCreate)
        logging.info("")
    else:
        os.mkdir(report_directory, 0o755)
        logging.info("%s Directory created", dirToCheckAndCreate)
        logging.info("")

    logging.info("--------------------------------------------------------------------------------------")
    logging.info("")

def exportTableDataToCSV(filePath, strSQLToExecute):
    moconfig.ora_cursor.arraysize = 100
    outputFileHandle = open(filePath, "w")
    writer = csv.writer(outputFileHandle, lineterminator="\n")
    writer.writerows(moconfig.ora_cursor.execute(strSQLToExecute))

def exportTableDataToCSVSP(filePath, strSQLToExecute):
    result_cursor = moconfig.ora_cursor.var(moconfig.cx_Oracle_cursor)
    moconfig.ora_cursor.callproc(strSQLToExecute, [result_cursor])
    result = result_cursor.getvalue()
    moconfig.ora_cursor.arraysize = 100
    outputFileHandle = open(filePath, "w")
    writer = csv.writer(outputFileHandle, lineterminator="\n")
    writer.writerows(result)

def exportTableDataToCSVWithHeader(filePath, strSQL):
    moconfig.ora_cursor.execute(strSQL)
    rows = moconfig.ora_cursor.fetchall()
    columns = [column[0].lower() for column in moconfig.ora_cursor.description]
    outputFile = open(filePath, "w")
    writer = csv.writer(outputFile, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)

def importTableDataFromCSV(filePath, strSQL, skipHeader = True, specialCharacter = ''):
   with  open(filePath, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        
        if skipHeader == True:
            #skip the header row
            next(csv_reader, None)

        #Convert remaining CSV rows to list of tuples
        if specialCharacter == '':
            data_to_insert = [tuple(row) for row in csv_reader]
        else:
            data_to_insert = [tuple(row[0].split(specialCharacter)) for row in csv_reader]
        
        moconfig.ora_cursor.executemany(strSQL,data_to_insert)
        moconfig.ora_connection.commit()

def readDataFromFile(filePath):
   with  open(filePath, 'r') as inputfile:
        fileData = inputfile.read()
        
        return fileData	

def checkAndRemoveDirectory(dirToCheckAndRemove, path):
    logging.info("--------------------------------------------------------------------------------------")
    logging.info("")

    logging.info("Check if %s Directory exists.", dirToCheckAndRemove)
    logging.info("")

    report_directory = os.path.join(path, dirToCheckAndRemove)

    dir_exists = os.path.exists(report_directory)

    if dir_exists:
        logging.info("Directory %s exists.", dirToCheckAndRemove)
        logging.info("")
        logging.info("Removing Directory %s.", dirToCheckAndRemove)
        logging.info("")
        os.system("rm -r " + report_directory)
    else:
        logging.info("%s Directory does not exist.", dirToCheckAndRemove)
        logging.info("")

    logging.info("--------------------------------------------------------------------------------------")
    logging.info("")


def convert_to_lowercase(filename):
    return filename.lower()

def file_exists_case_sensitive(filename):
    # check if file exists using case-sensitive filename matching
    if os.path.exists(moconfig.sftp_incoming_path + "/" + filename):
        # on case-insensitive file systems, check if the actual filename matches
        if os.path.basename(moconfig.sftp_incoming_path + "/" + filename) in os.listdir(moconfig.sftp_incoming_path):
            return True
    return False

def file_exists_case_sensitive_generic(filepath, filename):
    # check if file exists using case-sensitive filename matching
    if os.path.exists(filepath + "/" + filename):
        # on case-insensitive file systems, check if the actual filename matches
        if os.path.basename(filepath + "/" + filename) in os.listdir(filepath):
            return True
    return False
	
def overwrite_header(csv_file, old_header,new_header):
    # Open and read the contents of csv file 
    with  open(csv_file, 'rb') as readcsvfile:
        reader = csv.reader(readcsvfile)
        rows = list(reader)

        # Find the index of old header
        header_index = None
        if rows:
            if old_header in rows[0]:
                header_index = rows[0].index(old_header)

        if header_index is not None:
            # Overwrite the old header with new header
            rows[0][header_index] = new_header


            # Write back the modified contents to CSV file
            with open(csv_file,'wb') as overwritefile:
                csvwriter = csv.writer(overwritefile)
                csvwriter.writerows(rows)
        
            logging.info("Header overwritten successful.")
        else:
            logging.info("Old header not found.")

def runLinuxCommand(commandToRun):
    result = subprocess.check_output(commandToRun, shell=True)
    return result

def exportTableDataToCSVWithSeparator(filePath, strSQLToExecute, separator):
    moconfig.ora_cursor.arraysize = 100
    outputFileHandle = open(filePath, "w")
    writer = csv.writer(outputFileHandle, lineterminator="\n", delimiter=separator)
    writer.writerows(moconfig.ora_cursor.execute(strSQLToExecute))

def dos2unix(filename):
    with open(filename, 'rb') as f:
        content = f.read().replace(b'\r\n', b'\n')
   
    with open(filename, 'wb') as f:
        f.write(content)
		
def copyFile(source, destination):
	status = False
	try:
		shutil.copy(source, destination)
		status = True
	except Exception as e:
		logging.error(f"Error in copying the file from {source} to {description}. Reason:{e}")
	
	return status

def moveFile(source, destination):
	status = False
	try:
		shutil.move(source, destination)
		status = True
	except Exception as e:
		logging.error(f"Error in moving the file from {source} to {description}. Reason:{e}")
	
	return status
	
def executeSQL(sqlToExecute):
	status = False
	try:
		moconfig.ora_cursor.execute(sqlToExecute)	
	except Exception as e:
        logging.error(f"Error in executing query{sqlToExecute}. Reason:{e}")
        return status
    return True
