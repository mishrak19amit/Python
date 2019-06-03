import mysql.connector
import configparser
import SQL_cursor
import AppendToTable
import logging

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')
size = config['BATCH_DATA_FETCH']['size']
print("Batch size: " + str(size))
sqlhostname = config['DATA_MIGRATION_SOURCE']['HOSTNAME']
sqlusername = config['DATA_MIGRATION_SOURCE']['USERNAME']
sqlpassword = config['DATA_MIGRATION_SOURCE']['PASSWORD']
sqldbname = config['DATA_MIGRATION_SOURCE']['DBNAME']
sqltablename = config['DATA_MIGRATION_SOURCE']['TABLENAME']
logfile = config['DATA_MIGRATION_SOURCE']['logfile']

destsqlhostname = config['DATA_MIGRATION_DESTINATION']['HOSTNAME']
destsqlusername = config['DATA_MIGRATION_DESTINATION']['USERNAME']
destsqlpassword = config['DATA_MIGRATION_DESTINATION']['PASSWORD']
destsqldbname = config['DATA_MIGRATION_DESTINATION']['DBNAME']
destsqltablename = config['DATA_MIGRATION_DESTINATION']['TABLENAME']
supplierprofiletablename = config['DATA_MIGRATION_DESTINATION']['supplierprofiletablename']
supplieraddresstablename = config['DATA_MIGRATION_DESTINATION']['supplieraddresstablename']
suppliebussinesstypetablename = config['DATA_MIGRATION_DESTINATION']['SUPPLIEBUSSINESSTYPETABLENAME']
supplierbankstablename = config['DATA_MIGRATION_DESTINATION']['supplierbankstablename']
supplierdocumentstablename = config['DATA_MIGRATION_DESTINATION']['supplierdocumentstablename']
supplierdocumentsmapping = config['DATA_MIGRATION_DESTINATION']['supplierdocumentsmapping']
userauthtable = config['DATA_MIGRATION_DESTINATION']['userauthtable']
awsbaseurl = config['DATA_MIGRATION_DESTINATION']['awsbaseurl']
dirpath = config['DATA_MIGRATION_DESTINATION']['dirpath']

logging.basicConfig(filename=logfile, format='%(asctime)s %(levelname)s %(message)s', filemode='w')
logger = logging.getLogger()

# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

mydb = mysql.connector.connect(
    host=sqlhostname,
    user=sqlusername,
    passwd=sqlpassword,
    database=sqldbname,
    buffered=True
)

destmydb = mysql.connector.connect(
    host=destsqlhostname,
    user=destsqlusername,
    passwd=destsqlpassword,
    database=destsqldbname,
    buffered=True
)

# print(mydb)
countcur = mydb.cursor()
mycursor = mydb.cursor()
destcursur = destmydb.cursor()
destupdatecursur = destmydb.cursor()

countcur.execute("select count(*) as num from " + sqltablename)

rowcount = 0

logger.info(" Truncating all the target table!")

SQL_cursor.truncatetable(destmydb, destsqltablename)
SQL_cursor.truncatetable(destmydb, supplierprofiletablename)
SQL_cursor.truncatetable(destmydb, supplieraddresstablename)
SQL_cursor.truncatetable(destmydb, suppliebussinesstypetablename)
SQL_cursor.truncatetable(destmydb, supplierbankstablename)
SQL_cursor.truncatetable(destmydb, supplierdocumentstablename)
SQL_cursor.truncatetable(destmydb, supplierdocumentsmapping)
SQL_cursor.truncatetable(destmydb, userauthtable)


for x in countcur:
    # print("total records in table: " + str(x[0]))
    logger.info("total records in table: " + str(x[0]))
    rowcount = x[0]

rowbatch = rowcount / int(size)

listkey = [0, 2, 3, 28, 4, 30, 33, 17, 40, 36, 31, 26, 27]
listkeysuppliersprofiles = [0, 100, 6, 34, 8, 23, 26, 27]

inser_query = query = """INSERT INTO """ + destsqltablename + """ (id,unique_supplier_id, entity_name, contact_name, email, password, phone, verification_status, is_enable, is_email_verified, id_user_roles, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
inser_query_suppliersprofiles = query = """INSERT INTO """ + supplierprofiletablename + """ (  supplier_id,trade_name,alternate_email,alternate_phone,business_nature,gst_no,created_at,updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
rowcountbybatch = 1
for batchcount in range(rowbatch):
    query = "select * from " + sqltablename + " limit  " + str(batchcount * int(size)) + ", " + str(int(size))
    #print(query)
    mycursor.execute(query)
    result = mycursor.fetchall()
    vallist = []
    for dbdata in result:
        data = []
        checkgst = True
        for i in range(len(dbdata)):
            data.append(dbdata[i])
        if not data[26] and data[27]:
            data[26] = data[27]
        logger.info(" Updating id Num: " + str(data[0]))
        verificationstatus = 0
        # Inserting Data to Suppliers
        vallist = SQL_cursor.getvalulist(data, listkey)
        if (SQL_cursor.appendtotablewithreturn(destmydb, inser_query, vallist, logger)):
            logger.info("Data has been Inserted in suppliers")
            verificationstatus = 1
            AppendToTable.InsertUserAuth(destmydb,vallist, userauthtable, logger)
        else:
            logger.warning("Failed to insert data in suppliers")
        # Inserting to suppliersprofiles
        logger.info("Going to insert data in supplier Profile")
        vallistsuppliersprofiles = SQL_cursor.getvalulistsupplierprofiles(data, listkeysuppliersprofiles)
        if vallistsuppliersprofiles[5] is " ":
            checkgst = False
        isinsertedsupprofile = SQL_cursor.appendtotablewithreturn(destmydb, inser_query_suppliersprofiles,
                                                                  vallistsuppliersprofiles, logger)
        if isinsertedsupprofile:
            logger.info(" Data has been inserted in supplier profiles")
        else:
            logger.warning(" Data has not been inserted in supplier profiles")

        # Inserting to Supplier_Bussiness_Type
        logger.info("Going to insert data in Supplier_Bussiness_Type")
        isinsertedsupbussynesstype = AppendToTable.inserting_Supplier_Bussiness_Type(data, destmydb,
                                                                                     suppliebussinesstypetablename,
                                                                                     logger)
        if isinsertedsupbussynesstype:
            logger.info(" Data has been inserted in Supplier_Bussiness_Type ")
        else:
            logger.warning(" Data has not been inserted in Supplier_Bussiness_Type ")
        if verificationstatus is 1 and isinsertedsupprofile and isinsertedsupbussynesstype and checkgst:
            verificationstatus = 2
            logger.info(" Status changed to 2")
        # Inserting to supplier address
        logger.info("Going to insert data in Supplier Address")
        if (AppendToTable.inserting_Supplier_Address(data, mycursor, supplieraddresstablename, destmydb, logger)):
            if verificationstatus is 2:
                verificationstatus = 4
                logger.info(" Status changed to 4")

        # Inserting to supplier_banks
        logger.info("Going to insert data in supplier_banks")
        if (AppendToTable.Inserting_Supplier_Banks(data, mycursor, supplierbankstablename, destmydb, logger)):
            if verificationstatus is 4:
                verificationstatus = 5
                logger.info(" Status has been changed to 5")
        # Inserting to supplier_Documnets
        logger.info("Going to insert data in supplier_Documnets")
        if (
                AppendToTable.Inserting_Suppliers_Documents(data, mycursor, supplierdocumentstablename,supplierdocumentsmapping, destmydb,
                                                            awsbaseurl, dirpath, logger)):
            if verificationstatus is 5:
                logger.info(" Status has been changed to 10")
                verificationstatus = 10
        # commiting database
        destmydb.commit()
        old_verification_status = data[17]
        if (old_verification_status is 15) or (old_verification_status is 16):
            destupdatecursur.execute("UPDATE " + destsqltablename + " set verification_status=" + str(
                old_verification_status) + " Where id=" + str(data[0]))
            logger.info(" Final status: " + str(old_verification_status))
        else:
            updatequery = "UPDATE " + destsqltablename + " set verification_status=" + str(
                verificationstatus) + " Where id=" + str(data[0])
            print (updatequery)
            destupdatecursur.execute(
                "UPDATE " + destsqltablename + " set verification_status=" + str(
                    verificationstatus) + " Where id=" + str(data[0]))
            logger.info(" Final status: " + str(verificationstatus))

        del vallistsuppliersprofiles[:]
        del vallist[:]
        verificationstatus = 0
        destmydb.commit()
        logger.info(" Data has been updated for id " + str(data[0]))
        rowcountbybatch += 1
# mycursor= mydb.cursor()
# Incrementing for next remaining part
batchcount = batchcount + 1
query = "select * from " + sqltablename + " limit  " + str(batchcount * int(size)) + ", " + str(rowcount % int(size))
print query
print(" Entered into another field")
mycursor.execute(query)
result = mycursor.fetchall()
vallist = []
for dbdata in result:
    data = []
    checkgst = True
    for i in range(len(dbdata)):
        data.append(dbdata[i])
    if not data[26] and data[27]:
        data[26] = data[27]
    logger.info(" Updating id Num: " + str(data[0]))
    verificationstatus = 0
    # Inserting Data to Suppliers
    vallist = SQL_cursor.getvalulist(data, listkey)
    if (SQL_cursor.appendtotablewithreturn(destmydb, inser_query, vallist, logger)):
        logger.info("Data has been Inserted in suppliers")
        verificationstatus = 1
        AppendToTable.InsertUserAuth(destmydb, vallist, userauthtable, logger)
    else:
        logger.warning("Failed to insert data in suppliers")
    # Inserting to suppliersprofiles
    logger.info("Going to insert data in supplier Profile")
    vallistsuppliersprofiles = SQL_cursor.getvalulistsupplierprofiles(data, listkeysuppliersprofiles)
    if vallistsuppliersprofiles[5] is " ":
        checkgst = False
    isinsertedsupprofile = SQL_cursor.appendtotablewithreturn(destmydb, inser_query_suppliersprofiles,
                                                              vallistsuppliersprofiles, logger)
    if isinsertedsupprofile:
        logger.info(" Data has been inserted in supplier profiles")
    else:
        logger.warning(" Data has not been inserted in supplier profiles")

    # Inserting to Supplier_Bussiness_Type
    logger.info("Going to insert data in Supplier_Bussiness_Type")
    isinsertedsupbussynesstype = AppendToTable.inserting_Supplier_Bussiness_Type(data, destmydb,
                                                                                 suppliebussinesstypetablename,
                                                                                 logger)
    if isinsertedsupbussynesstype:
        logger.info(" Data has been inserted in Supplier_Bussiness_Type ")
    else:
        logger.warning(" Data has not been inserted in Supplier_Bussiness_Type ")
    if verificationstatus is 1 and isinsertedsupprofile and isinsertedsupbussynesstype and checkgst:
        verificationstatus = 2
        logger.info(" Status changed to 2")
    # Inserting to supplier address
    logger.info("Going to insert data in Supplier Address")
    if (AppendToTable.inserting_Supplier_Address(data, mycursor, supplieraddresstablename, destmydb, logger)):
        if verificationstatus is 2:
            verificationstatus = 4
            logger.info(" Status changed to 4")

    # Inserting to supplier_banks
    logger.info("Going to insert data in supplier_banks")
    if (AppendToTable.Inserting_Supplier_Banks(data, mycursor, supplierbankstablename, destmydb, logger)):
        if verificationstatus is 4:
            verificationstatus = 5
            logger.info(" Status has been changed to 5")
    # Inserting to supplier_Documnets
    logger.info("Going to insert data in supplier_Documnets")
    if (
            AppendToTable.Inserting_Suppliers_Documents(data, mycursor, supplierdocumentstablename,
                                                        supplierdocumentsmapping, destmydb,
                                                        awsbaseurl, dirpath, logger)):
        if verificationstatus is 5:
            logger.info(" Status has been changed to 10")
            verificationstatus = 10
    # commiting database
    destmydb.commit()
    old_verification_status = data[17]
    if (old_verification_status is 15) or (old_verification_status is 16):
        destupdatecursur.execute("UPDATE " + destsqltablename + " set verification_status=" + str(
            old_verification_status) + " Where id=" + str(data[0]))
        logger.info(" Final status: " + str(old_verification_status))
    else:
        updatequery = "UPDATE " + destsqltablename + " set verification_status=" + str(
            verificationstatus) + " Where id=" + str(data[0])
        print (updatequery)
        destupdatecursur.execute(
            "UPDATE " + destsqltablename + " set verification_status=" + str(
                verificationstatus) + " Where id=" + str(data[0]))
        logger.info(" Final status: " + str(verificationstatus))

    del vallistsuppliersprofiles[:]
    del vallist[:]
    verificationstatus = 0
    destmydb.commit()
    logger.info(" Data has been updated for id " + str(data[0]))
    rowcountbybatch += 1
mycursor.close()
