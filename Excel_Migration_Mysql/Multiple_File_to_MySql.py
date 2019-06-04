import glob
import mysql.connector
import xlrd
import os
import configparser
from datetime import datetime


class FileMerger:

    def __init__(self):
        print("Excel to Mysql Migration Utility  started")

# Inserting excel data in Mysql Table###

    def inserDataToMySql(self, cursor, filename, teamname):
        # Open the workbook and define the worksheet
        book = xlrd.open_workbook(filename)
        sheet = book.sheet_by_index(0)
        colrange = sheet.ncols
        if colrange > 65:
            colrange = 65

        # Create the INSERT INTO sql query
        query = """INSERT IGNORE INTO """+sqltablename +""" (Plant_Name,Plant_ID,LD,PO_No,User_Purchaser_Name,Warehouse,PO_Document_Date,PO_Received_Date,Customer_Due_Delivery_Date,CPN_Code,Material_Description_Short_Text,Material_Description_Long_Text,PO_Status,Qty,UOM,SP_Unit_in_PO,TAX_percent,Total_Pre_tax_value,Total_Customer_Invoice_Value,Customer_Freight_Terms,Buyers_PO_punching_date,Buyers_customer_item_id,Sourcing_Allocation,Customer_LPP_Contract_1_Price,Customer_LPP_Contract_2_Price,Deviation_from_LPP_Contract_Price,Sourcing_KAM_Remarks,Moglix_Supplier,Supplier_ID,HSN,HSN_TAX,Brand,List_Price,Discpercent_Received,TP_Unit,Supplier_Freight_Terms,Supplier_Advance_Credit,Supplier_Credit_days_in_days,Supplier_Pick_up_due_date,MSN,MSN_Description,Supplier_PO_ID,EMS_Item_ID,Supplier_PO_raised_date,OPS_Allocation,OPS_team_remarks,Supplier_committed_pick_up_date,Actual_pick_up_date_MRN_date,Inbound_freight_cost,Invoice_Number,Invoice_Date,Delivered_date,Outbound_Freight_Cost,CN_Raised,Customer_Credit_Days,GMpercent,GM_mul_percent,PO_date_vs_PO_recived_TAT,PO_recvd_vs_PO_Punching_TAT,PO_Punching_vs_Supplier_PO_TAT,Committed_vs_Actual_Pick_UP_Date_TAT,Pick_Up_Date_vs_Invoice_Date_TAT,Invoice_Date_vs_Delivery_Date_TAT,Delivery_TAT,Moglix_ETA, teamname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        # print(query)
        Val_ist = []
        # Create a For loop to iterate through each row in the XLS file, starting at row 2 to skip the headers
        for r in range(1, sheet.nrows):
            # print(sheet.ncols)
            ponum = sheet.cell(r, 3).value
            if ponum:
                for l in range(0, colrange):
                    val = sheet.cell(r, l).value
                    if not val:
                        val = None
                    elif l in [6,7,8,20,38,43,46,47,50,51,64]:
                        try:
                            excel_date = int(val)
                            dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + excel_date - 2)
                            val = str(dt.year) + "-" + str(dt.month) + "-" + str(dt.day)
                        except Exception:
                            val=None
                            #print(val)
                    Val_ist.append(val)
                    #print(Val_ist)
                Val_ist.append(teamname)
                cursor.execute(query, Val_ist)
                del Val_ist[:]
        # Print results
        print("")
        print("All Done for this file!")
        print("")
        os.rename(filename, filename + ".done")


objclass = FileMerger()
config = configparser.ConfigParser()
config.read('config.ini')

dirname = config['FILE_DOWNLOAD']['DIR_NAME']
sqlhostname=config['EXCEL_UPLOAD_MYSQL']['HOSTNAME']
sqlusername=config['EXCEL_UPLOAD_MYSQL']['USERNAME']
sqlpassword=config['EXCEL_UPLOAD_MYSQL']['PASSWORD']
sqldbname=config['EXCEL_UPLOAD_MYSQL']['DBNAME']
sqltablename=config['EXCEL_UPLOAD_MYSQL']['TABLENAME']
# Directory where Excel file exist
# dirname ="/home/moglix/Desktop/Dev_Documetns/Input/Excel_File/Sample"

# Establish a MySQL connection
mydb = mysql.connector.connect(host=sqlhostname, user = sqlusername, passwd = sqlpassword, db = sqldbname)

# Get the cursor, which is used to traverse the database, line by line
cursor = mydb.cursor()

cursor.execute("TRUNCATE TABLE " + sqltablename)

# print(glob.glob(path+"/*.xlsx"))
#filelist = glob.glob(dirname+"/*.xlsx")
filelist=open('/home/moglix/Desktop/Dev_Documetns/Input/Excel_File/Team_Name.txt', 'r')

for line in filelist:
    listval=line.split("=")
    filename=dirname+"/"+listval[0]
    teamname=listval[1].strip()
    print filename
    try:
        objclass.inserDataToMySql(cursor, filename, teamname)
    except Exception:
        print("File does not have specified schema")
        # Commit the transaction
    mydb.commit()

    # closing the Db connection
print("Excel has been uploaded to Mysql Table")
cursor.close()
mydb.close()