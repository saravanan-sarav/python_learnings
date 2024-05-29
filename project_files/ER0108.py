
import os
import subprocess
import logging
import cx_Oracle
from datetime import datetime
import moconfig
import sys
import helperfunctions
from helperfunctions import * ####new change
# Initialize logging
log_file_name = os.path.join(os.path.dirname(__file__), 'RDBW672_{}.log'.format(datetime.now().strftime('%Y%m%d')))
logging.basicConfig(filename=log_file_name, level=logging.INFO, format='%(asctime)s - %(message)s')

# Environment variables and directory setup
CURDIR = os.getcwd()
WGDIR = os.path.dirname(CURDIR)
REPTDIR = os.path.dirname(WGDIR)
AST_SCRIPTS = WGDIR
os.environ['CURDIR'] = CURDIR
os.environ['WGDIR'] = WGDIR
os.environ['REPTDIR'] = REPTDIR
os.environ['AST_SCRIPTS'] = AST_SCRIPTS



RDB_FOLDER = "RDBW672"
APFILENAME = "RDBW672_{}".format(datetime.now().strftime('%Y%m%d'))

os.chdir(os.path.join(AST_SCRIPTS, RDB_FOLDER))
LOG = os.path.join(AST_SCRIPTS, RDB_FOLDER, os.path.basename(__file__) + '.log')
os.environ['LOG'] = LOG
# Process name variable
PROCESS = os.path.basename(__file__).split('.')[0]
logging.info(PROCESS)

inFile = 'client_profile_managed_account.txt'
inFile1 = 'client_profile_managed_account_new.txt'
inFile2 = 'representative_match.csv'
inFile3 = 'branch_QR.csv'

sftp_in=AST_SCRIPTS+'/sftp/incoming' #new change
DATAPATH=AST_SCRIPTS+'/data_files'

def dropTable( table_name, suppress_error=True):
    statement = ""
    if suppress_error:
        statement = """
        BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE {}';
        EXECUTE IMMEDIATE 'DROP TABLE {}';
        EXCEPTION
        WHEN OTHERS THEN
            IF sqlcode != -0942 THEN RAISE; 
            END IF;
        END;
        """.format(table_name,table_name)
    else:
        statement = "drop table {}".format( table_name)
    if not executeSQL(statement):
        logging.error("Error in executing sql")

# Beginning of program
logging.info("Started ER0108 reports at {}".format(datetime.now()))

sql_to_execute="""BEGIN
EXECUTE IMMEDIATE 'DROP TABLE client_accnt_input';
EXCEPTION
WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
        RAISE;
    END IF;
END;"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)
sql_to_execute ="""
CREATE TABLE client_accnt_input
(
branch VARCHAR2(3),
branch_name VARCHAR2(30),
representative VARCHAR2(3),
representative_name VARCHAR2(50),
client_id NUMBER,
client_name VARCHAR2(100),
account_id VARCHAR2(20),
date_added VARCHAR2(8),
inv_obj_income VARCHAR2(10),
inv_obj_gs VARCHAR2(10),
inv_obj_gm VARCHAR2(10),
inv_obj_gl VARCHAR2(10),
risk_tol_low VARCHAR2(10),
risk_tol_med VARCHAR2(10),
risk_tol_high VARCHAR2(10)
)"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)
sql_to_execute="""
BEGIN
EXECUTE IMMEDIATE 'DROP TABLE representative_match';
EXCEPTION
WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
        RAISE;
    END IF;
END;"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)
sql_to_execute="""
CREATE TABLE representative_match
(
orig_representative VARCHAR2(6),
representative VARCHAR2(6),
fckey VARCHAR2(6),
orig_branch VARCHAR2(8),
branch VARCHAR2(100),
branchCode VARCHAR2(100),
region VARCHAR2(100),
owner_domain VARCHAR2(100),
owner_id VARCHAR2(100),
folder_id VARCHAR2(255)
)"""

if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)

sql_to_execute="""BEGIN
EXECUTE IMMEDIATE 'DROP TABLE branch_QR';
EXCEPTION
WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
        RAISE;
    END IF;
END;"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)
sql_to_execute="""
CREATE TABLE branch_QR
(
MO_branch VARCHAR2(50),
branchCode VARCHAR2(50),
MO_branch_name VARCHAR2(500)
)"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)
if file_exists_case_sensitive_generic(sftp_in,inFile):
    logging.info(f"File {inFile} exists")
else:
    logging.info(f"File {inFile} does not exist. Exiting")
    sys.exit(105)  #new change

if not copyFile(f"{sftp_in}/{inFile}", f"./{inFile}"):#new change remove $
    logging.error(f"Copy of ${inFile} failed!")
    sys.exit(105)


dos2unix(inFile)
curr_date = datetime.now().strftime('%Y%m%d')
logging.info("Current business date is {}".format(curr_date))

# Check the date in the file. It should be the current business date.
logging.info("Getting the header date in the file")

with open(inFile, 'r') as input_file:
    first_line = input_file.readline()
with open('ER0108_date.txt', 'w') as output_file:
    output_file.write(first_line)
with open('ER0108_date.txt', 'r') as in_file:
    header = in_file.readline().split('|')[1].strip()
    
logging.info("The header date of the file is {}".format(header))

if header != curr_date:
    logging.error("Error: Invalid input file as it doesn't contain the current business date in the file header")
    #exit(2)
else:
    logging.info("The input file is correct and contains the current business date")

# Get record count in trailer record
logging.info("Getting the record count in the trailer of the file")
with open(inFile, 'r') as f:
    for line in f:
        pass
    trailer = line.split('|')[1].strip()
logging.info("The count in the trailer is {}".format(trailer))

# Remove header and footer from input files
logging.info("Deleting header and trailer from client_profile_managed_account.txt")
try:
    with open(inFile, 'r') as f, open('temp_file.txt', 'w') as temp_file:
        lines = f.readlines()[2:-1]  # Skip first two lines and the last line
        temp_file.writelines(lines)
except Exception as e:
    logging.error("Error #105d: delete header and footer In {} Failed".format(inFile))
    #exit(105)

# Removing the first column from the input file which is the record indicator
logging.info("Deleting the indicator field from the temp file")
try:
    with open('temp_file.txt', 'r') as temp_file, open(inFile1, 'w') as final_file:
        for line in temp_file:
            final_file.write('|'.join(line.split('|')[1:]))
except Exception as e:
    logging.error("Error #105d: delete indicator field In {} Failed".format(inFile1))
    #exit(105)

# Check rowcount of the input file
logging.info("Getting the record count of the file")
with open(inFile1, 'r') as f:
    x = sum(1 for _ in f)
logging.info("The record count of the file {} is {}".format(inFile1, x))

if str(trailer) != str(x):
    logging.error("Error: Invalid record count in client_profile_managed_account.txt")
    #exit(2)
else:
    logging.info("Valid record count in client_profile_managed_account.txt")

strClientAccntInputInsertQuery = "insert into client_accnt_input values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15) "
if not importTableDataFromCSV(inFile1,strClientAccntInputInsertQuery, False, "|"):
    logging.error("Error: bcp in for {$inFile1}")
    sys.exit(103)
logging.info("bcp in {} worked.".format(inFile1))

if file_exists_case_sensitive_generic(sftp_in,inFile2):
    logging.info(f"File {inFile2} exists")
else:
    logging.info(f"File {inFile2} does not exist. Exiting")
    sys.exit(105)

if not copyFile(f"{sftp_in}/{inFile2}", f"./{inFile2}"):
    logging.error(f"Copy of ${inFile2} failed!")
    sys.exit(105)

dos2unix(inFile2)
strRepresentativeMatchInsertQuery = "insert into representative_match values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10) "
if not importTableDataFromCSV(inFile2, strRepresentativeMatchInsertQuery, False, "^@"):
    logging.error("Error: bcp in for {$inFile2}")
    sys.exit(103)
logging.info("bcp in {} worked.".format(inFile2))

if file_exists_case_sensitive_generic(sftp_in,inFile3):
    logging.info(f"File {inFile3} exists")
else:
    logging.info(f"File {inFile3} does not exist. Exiting")
    sys.exit(105)

if not copyFile(f"{sftp_in}/{inFile3}", f"./{inFile3}"):
    logging.error(f"Copy of ${inFile3} failed!")
    sys.exit(105)

dos2unix(inFile3)
strBranchQRInsertQuery = "insert into branch_QR values (:1,:2,:3,:4) "
if not importTableDataFromCSV('branch_QR.csv', strBranchQRInsertQuery, True, ','):
    logging.error("Error: bcp in for {branch_QR.csv}")
logging.info("bcp in {} worked.".format(inFile3))

sql_to_execute ="""
UPDATE client_accnt_input
SET account_id = '00' || SUBSTR(account_id,1,3) || '00000' || SUBSTR(account_id,5,5)"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)

dropTable('latest_date')
sql_to_execute="""
CREATE TABLE latest_date AS
SELECT currency, MAX(fx_rate_date) fx_rate_date
FROM exchange_rate
WHERE fx_rate_date <= (SELECT business_date_current FROM system_control)
GROUP BY currency
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")

# -- Assuming error_check_sp and related logic is handled differently in Oracle as there's no direct @@error, @@rowcount in Oracle.
dropTable('latest_rate')
sql_to_execute="""
CREATE TABLE latest_rate  AS
    SELECT l.currency, e.fx_spot_rate
    FROM latest_date l JOIN exchange_rate e ON l.currency = e.currency AND l.fx_rate_date = e.fx_rate_date
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")

sql_to_execute="""
INSERT INTO latest_rate (currency, fx_spot_rate)
VALUES ('CAD', 1)
"""
#if not executeSQL(sql_to_execute):
    #   logging.error("Error in executing sql")

sql_to_execute="""
CREATE INDEX idx_curr ON latest_rate(currency)
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")

dropTable( 'holdings')
sql_to_execute="""
CREATE TABLE holdings AS
    SELECT DISTINCT a.account_id, tr.ti, tr.currency, tr.curr_mkt_value
    FROM tran_summ tr JOIN client_accnt_input a ON a.account_id = tr.account_id
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")

sql_to_execute="""
CREATE INDEX indx_ti ON holdings(ti)
    """
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")

# --market value
sql_to_execute="""
UPDATE holdings h
SET h.curr_mkt_value = (SELECT NVL((h.curr_mkt_value * lr.fx_spot_rate),0)
                        FROM latest_rate lr
                        WHERE h.currency = lr.currency)
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    
# --Total holdings
dropTable( 'hold')
sql_to_execute="""
CREATE TABLE hold  AS
    SELECT account_id, NVL(SUM(curr_mkt_value), 0) AS total_holdings_cdn
    FROM holdings
    GROUP BY account_id
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")

# --get Total Cash Balance for accounts
dropTable( 'cash')
sql_to_execute="""
CREATE TABLE cash  AS
    SELECT a.account_id, NVL(SUM(r.fx_spot_rate * mm.td_net_amt * -1),0) total_cash_cdn
    FROM client_accnt_input a, sub_accnt_summ mm, latest_rate r
    WHERE mm.currency = r.currency AND mm.account_id = a.account_id
    GROUP BY a.account_id
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")

sql_to_execute="""
ALTER TABLE client_accnt_input
ADD total_asset NUMBER(20,2)
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")

# -- get total asset for accounts into final table
sql_to_execute="""
MERGE
INTO    client_accnt_input ci
USING   (
    SELECT ci.account_id, NVL((h.total_holdings_cdn + c.total_cash_cdn),0) AS val
                    FROM client_accnt_input ci JOIN hold h  ON ci.ACCOUNT_ID =h.account_id FULL JOIN cash c ON ci.account_id = h.account_id AND ci.account_id = c.account_id
                    WHERE ci.account_id = h.account_id OR ci.account_id = c.account_id
    ) src
ON      (ci.account_id = src.account_id)
WHEN MATCHED THEN UPDATE
SET ci.TOTAL_ASSET  = src.val
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")

sql_to_execute="""
UPDATE client_accnt_input
SET account_id = SUBSTR(account_id,3,3) || '-' || SUBSTR(account_id,11,5)
    """
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)

# # Parse commas
strClientAccntInputER0108ParseCommas = """update client_accnt_input
                                            set branch_name = replace(branch_name, ',', ' '), 
                                            representative_name = replace(representative_name, ',', ' '),
                                            client_name = replace(client_name, ',', ' ')"""

if not executeSQL(strClientAccntInputER0108ParseCommas):
    logging.error("Error in executing sql")
    sys.exit(101)
# logging.info("Create RDBW672 report to store the report on NAS drive")
exportTableDataToCSVWithHeader(AST_SCRIPTS + "/" + APFILENAME+'.csv', "select BRANCH,BRANCH_NAME,REPRESENTATIVE,REPRESENTATIVE_NAME,CLIENT_ID,CLIENT_NAME,ACCOUNT_ID,DATE_ADDED,INV_OBJ_INCOME,INV_OBJ_GS,INV_OBJ_GM,INV_OBJ_GL,RISK_TOL_LOW,RISK_TOL_MED,RISK_TOL_HIGH,cast( TOTAL_ASSET as integer) as TOTAL_ASSET from client_accnt_input")


strQR_Standard =   """
                    create table QR_Standard (
                        reportId int NULL, fckey int NULL, branchCode int NULL, regionCode char(1) NULL, 
                        fielda varchar(100) NULL, fieldb varchar(100) NULL, fieldc varchar(100) NULL, 
                        fieldd varchar(100) NULL, fielde varchar(100) NULL, fieldf varchar(100) NULL, 
                        fieldg varchar(100) NULL, fieldh varchar(100) NULL, fieldi varchar(100) NULL, 
                        fieldj varchar(100) NULL, fieldk varchar(100) NULL, fieldl varchar(100) NULL, 
                        fieldm varchar(100) NULL, fieldn varchar(100) NULL, fieldo varchar(100) NULL, 
                        fieldp varchar(100) NULL, fieldq varchar(100) NULL, fieldr varchar(100) NULL, 
                        fields varchar(100) NULL, fieldt varchar(100) NULL)"""

dropCreateTempTables("QR_Standard", strQR_Standard)

logging.info("Calling dropCreateTempTables helper function to check, drop and create strQR_Standard table is successful.")
logging.info("")

sql_to_execute = """
BEGIN
    EXECUTE IMMEDIATE 'create index ind_id_qrs on QR_Standard(reportId)';
END;"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)

strQR_Date =   """
                    create table QR_Date (reportId int NULL, report_date	varchar(20) NULL)"""

dropCreateTempTables("QR_Date", strQR_Date)
sql_to_execute = """
BEGIN
    EXECUTE IMMEDIATE 'create index ind_id_qrd on QR_Date(reportId)';
END;"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)

if not file_exists_case_sensitive_generic(f"{DATAPATH}","QR_Standard.txt"):
    with open(f"{DATAPATH}/QR_Standard.txt", 'w') as f:
        os.chmod(f"{DATAPATH}/QR_Standard.txt", 0o777)

if not file_exists_case_sensitive_generic(f"{DATAPATH}","QR_Date.txt"):
    with open(f"{DATAPATH}/QR_Date.txt", 'w') as f:
        os.chmod(f"{DATAPATH}/QR_Date.txt", 0o777)

strQRStandardInsertQuery = "insert into QR_Standard values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,;20,:21,:22,:23) "
if not importTableDataFromCSV(f"{DATAPATH}/QR_Standard.txt",strQRStandardInsertQuery, False, "^@"):
    logging.error("Error: bcp in for {DATAPATH}/QR_Standard.txt")
    

logging.info("bcp in {}/QR_Standard.txt worked.".format(DATAPATH))

strQRDateInsertQuery = "insert into QR_Date values (:1,:2) "
if not importTableDataFromCSV(f"{DATAPATH}/QR_Date.txt",strQRDateInsertQuery, False, "^@"):
    logging.error("Error: bcp in for {DATAPATH}/QR_Date.txt")
    sys.exit(103)

logging.info("bcp in {}/QR_Date.txt worked.".format(DATAPATH))


dropTable('ER0108_final')
sql_to_execute = """
--Oracle equivalent statements
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE ER0108_final  AS SELECT 
        CAST(rm.fckey AS NUMBER(38)) AS fckey,
        CAST(rm.branchCode AS NUMBER(38)) AS branchCode,
        c.branch,
        c.representative,
        c.representative_name,
        TO_CHAR(c.client_id) AS client_id,
        c.client_name,
        c.account_id,
        c.date_added,
        c.inv_obj_income,
        c.inv_obj_gs,
        c.inv_obj_gm,
        c.inv_obj_gl,
        c.risk_tol_low,
        c.risk_tol_med,
        c.risk_tol_high,
        TO_CHAR(c.total_asset) AS total_asset
    FROM client_accnt_input c
    LEFT JOIN representative_match rm ON c.representative = rm.orig_representative
    ORDER BY c.client_id, c.account_id';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN
        NULL; -- ignore error if table exists
    ELSE
        RAISE;
    END IF;
END;
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)

sql_to_execute="""
UPDATE ER0108_final a
SET a.branchCode = (SELECT CAST(br.branchCode AS NUMBER(38)) 
                    FROM branch_QR br 
                    WHERE a.branch = br.MO_branch)
WHERE a.branchCode IS NULL
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)
dropTable('split_codes')

sql_to_execute="""
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE split_codes  AS SELECT account_id
    FROM ER0108_final
    GROUP BY account_id
    HAVING COUNT(account_id) > 1';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN
        NULL; -- ignore error if table exists
    ELSE
        RAISE;
    END IF;
END;
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)

sql_to_execute="""
UPDATE ER0108_final a
SET a.branchCode = NULL
WHERE a.account_id IN (SELECT b.account_id FROM split_codes b)
AND a.fckey NOT IN (
    SELECT MAX(fckey) FROM ER0108_final
    GROUP BY account_id HAVING COUNT(account_id) > 1
)
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)

sql_to_execute="""
DELETE FROM QR_Standard
WHERE reportId IN (108)
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)
sql_to_execute="""
INSERT INTO QR_Standard
SELECT 108,
    fckey,
    branchCode,
    NULL,
    representative,
    representative_name,
    client_id,
    client_name,
    account_id, 
    date_added,
    inv_obj_income,
    inv_obj_gs,
    inv_obj_gm, 
    inv_obj_gl,
    risk_tol_low,
    risk_tol_med,
    risk_tol_high,
    TO_CHAR(total_asset),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
FROM ER0108_final
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)
sql_to_execute="""
INSERT INTO QR_Date
SELECT 108, TO_CHAR(SYSDATE, 'MM/DD/YYYY')
FROM dual
"""
if not executeSQL(sql_to_execute):
    logging.error("Error in executing sql")
    sys.exit(101)

# # Run QR_Standard_bcp_out.ksh
logging.info("Run QR_Standard_bcp_out.ksh")
logging.info("KSHPATH :  {}".format(os.getenv('KSHPATH')))
logging.info("AST_SCRIPTS :  {}".format(AST_SCRIPTS))

if not file_exists_case_sensitive_generic(f"{DATAPATH}","QR_Standard.txt"):
    with open(f"{DATAPATH}/QR_Standard.txt", 'w') as f:
        os.chmod(f"{DATAPATH}/QR_Standard.txt", 0o777)

if not file_exists_case_sensitive_generic(f"{DATAPATH}","QR_Date.txt"):
    with open(f"{DATAPATH}/QR_Date.txt", 'w') as f:
        os.chmod(f"{DATAPATH}/QR_Date.txt", 0o777)

logging.info("Begin BCP OUT QR_Standard and QR_Date at {}".format(datetime.now()))

strExportQRStandardQuery = """  select   to_char(REPORTID) || '^', coalesce(to_char(FCKEY), '') || '^',
                            coalesce(to_char(BRANCHCODE), '') || '^', coalesce(to_char(REGIONCODE), '') || '^',
                            coalesce(to_char(FIELDA), '') || '^', coalesce(to_char(FIELDB), '') || '^',
                            coalesce(to_char(FIELDC), '') || '^', coalesce(to_char(FIELDD), '') || '^',
                            coalesce(to_char(FIELDE), '') || '^', coalesce(to_char(FIELDF), '') || '^',
                            coalesce(to_char(FIELDG), '') || '^', coalesce(to_char(FIELDH), '') || '^',
                            coalesce(to_char(FIELDI), '') || '^', coalesce(to_char(FIELDJ), '') || '^',
                            coalesce(to_char(FIELDK), '') || '^', coalesce(to_char(FIELDL), '') || '^',
                            coalesce(to_char(FIELDM), '') || '^', coalesce(to_char(FIELDN), '') || '^',
                            coalesce(to_char(FIELDO), '') || '^', coalesce(to_char(FIELDP), '') || '^',
                            coalesce(to_char(FIELDQ), '') || '^', coalesce(to_char(FIELDR), '') || '^',
                            coalesce(to_char(FIELDS), '') || '^', coalesce(to_char(FIELDT), '')  
                            from QR_STandard """
if not  exportTableDataToCSVWithSeparator(f"{DATAPATH}/QR_Standard.txt",strExportQRStandardQuery, "@"):
    logging.error("Error: bcp out for $temp_mis..QR_Standard")
    sys.exit(103)

strExportQRDateQuery =  "select to_char(REPORTID) || '^',  REPORT_DATE from QR_DATE"
if not exportTableDataToCSVWithSeparator(f"{DATAPATH}/QR_Date.txt",strExportQRDateQuery, "@"):
    logging.error("Error: bcp out for $temp_mis..QR_Date")
    sys.exit(103)

logging.info("**********************************************************************")
logging.info(" BCP OUT QR_Standard process completed successfully at {}".format(datetime.now()))
logging.info("**********************************************************************")
sql_to_execute = """
BEGIN
EXECUTE IMMEDIATE 'DROP TABLE client_accnt_input';
EXCEPTION
WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
        RAISE;
    END IF;
END;
"""
# if not executeSQL(sql_to_execute):
#     logging.error("Error in executing sql")
#     sys.exit(101)
sql_to_execute="""
BEGIN
EXECUTE IMMEDIATE 'DROP TABLE representative_match';
EXCEPTION
WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
        RAISE;
    END IF;
END;
"""
# if not executeSQL(sql_to_execute):
#     logging.error("Error in executing sql")
#     sys.exit(101)

APFILENAME2 = "CPMA_ER0108_NL_{}".format(datetime.now().strftime('%Y%m%d'))

# # Convert csv file to pipe delimited and move to out_qr_nas directory
logging.info("Convert csv file to pipe delimited.")
with open(os.path.join(AST_SCRIPTS, '{}.csv'.format(APFILENAME)), 'r') as csv_file, open(AST_SCRIPTS+'/sftp/outgoing/out_ext/{}.txt'.format(APFILENAME2), 'w') as pipe_file:
    for line in csv_file:
        pipe_file.write(line.replace(',', '|'))

# # Removing all the temporary files
logging.info("Removing all the temporary files")
try:
    os.remove(os.path.join(AST_SCRIPTS, RDB_FOLDER, 'client_profile_managed_account.txt'))
    os.remove(os.path.join(AST_SCRIPTS, RDB_FOLDER, 'client_profile_managed_account_new.txt'))
    os.remove(os.path.join(AST_SCRIPTS, RDB_FOLDER, 'branch_QR.csv'))
    os.remove(os.path.join(AST_SCRIPTS, RDB_FOLDER, 'representative_match.csv'))
    os.remove(os.path.join(AST_SCRIPTS, RDB_FOLDER, 'temp_file.txt'))
    os.remove(os.path.join(AST_SCRIPTS, RDB_FOLDER, 'ER0108_date.txt'))
except Exception as e:
    logging.error(" - ** ERROR: Copy of {}.csv failed!".format(APFILENAME))
    exit(1)





