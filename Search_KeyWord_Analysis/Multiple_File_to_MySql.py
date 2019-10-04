import glob
import mysql.connector
import xlrd
import os
import configparser
from datetime import datetime


class FileMerger:

    def __init__(self):
        print("searchKeyword to Mysql Migration Utility  started")

# Inserting excel data in Mysql Table###

    def inserDataToMySql(self, cursor, filename):
        # Open the workbook and define the worksheet
        book = xlrd.open_workbook(filename)
        sheet = book.sheet_by_index(1)
        colrange = sheet.ncols
        print(colrange)

        # Create the INSERT INTO sql query
        query = """INSERT IGNORE INTO """+sqltablename +""" ( keyword, total_Unique_Searches, results_Pageviews_Search, search_Exits, search_Refinements, time_After_Search, avg_Search_Depth) VALUES ( %s, %s, %s, %s, %s, %s, %s)"""
        # print(query)
        Val_ist = []
        # Create a For loop to iterate through each row in the XLS file, starting at row 2 to skip the headers
        for r in range(1, sheet.nrows):
            # print(sheet.ncols)
                for l in range(0, colrange):
                    val = sheet.cell(r, l).value
                    Val_ist.append(val)
                    #print(Val_ist)
                cursor.execute(query, Val_ist)
        # Print results
        print("")
        print("All Done for this file!")
        print("")
        os.rename(filename, filename + ".done")


objclass = FileMerger()
config = configparser.ConfigParser()
config.read('config.ini')

dirname = config['FILE_DOWNLOAD']['DIR_NAME']
sqlhostname=config['KEYWORD_UPLOAD_MYSQL']['HOSTNAME']
sqlusername=config['KEYWORD_UPLOAD_MYSQL']['USERNAME']
sqlpassword=config['KEYWORD_UPLOAD_MYSQL']['PASSWORD']
sqldbname=config['KEYWORD_UPLOAD_MYSQL']['DBNAME']
sqltablename=config['KEYWORD_UPLOAD_MYSQL']['TABLENAME']
# Directory where Excel file exist
# dirname ="/home/moglix/Desktop/Dev_Documetns/Input/Excel_File/Sample"

# Establish a MySQL connection
mydb = mysql.connector.connect(host=sqlhostname, user = sqlusername, passwd = sqlpassword, db = sqldbname)

# Get the cursor, which is used to traverse the database, line by line
cursor = mydb.cursor()

cursor.execute("TRUNCATE TABLE " + sqltablename)

# print(glob.glob(path+"/*.xlsx"))
filelist = glob.glob(dirname+"/*.xlsx")
#filelist=open('/home/moglix/Desktop/Dev_Documetns/Input/Excel_File/Team_Name.txt', 'r')

for filename in filelist:

    print(filename)
    try:
        objclass.inserDataToMySql(cursor, filename)
    except Exception as e:
        print("File does not have specified schema", e)
        # Commit the transaction
    mydb.commit()

    # closing the Db connection
print("Excel has been uploaded to Mysql Table")
cursor.close()
mydb.close()