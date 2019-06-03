import mysql.connector
import re
import string
import random


def mysqldb(hostname, username, password, database):
    mydb = mysql.connector.connect(
        host=hostname,
        user=username,
        passwd=password,
        database=database
    )
    return mydb

def appendtotable(mydb, query, vallist,logger):
    try:
        mycursur = mydb.cursor()
        mycursur.execute(query, vallist)
    except Exception as e:
        print (vallist)
        #print "Data is not valid "
        print (e)
        logger.error(str(e))

def appendtotablewithreturn(mydb, query, vallist,logger):
    try:
        mycursur = mydb.cursor()
        mycursur.execute(query, vallist)
    except Exception as e:
        print (vallist)
        print "Data is not valid "
        print (e)
        logger.error(str(e))
        return False
    return True

def truncatetable(mydb, tablename):
    mycursor = mydb.cursor()
    mycursor.execute("truncate table " + tablename)
    print (tablename + " truncated successfully")


def gettrimmedphone(val):
    if val:
        m = re.search('(?<=-)\w+', val)
        if m:
            val = m.group(0)
            # print (val)
        else:
            val = 0
            # print(val)
    return val


def getvalulist(data, listkey):
    vallist = []
    for i in listkey:
        val = data[i]
        if i in [33]:
            # print (i)
            val = gettrimmedphone(data[i])
        elif i in [28]:
            lname = (data[i + 1])
            lname = re.sub(r'[^\x00-\x7F]+', '', lname)
            fname = (data[i])
            fname = re.sub(r'[^\x00-\x7F]+', '', fname)
            val = str(fname.strip()) + " " + str(lname.strip())
            # print (val)
        if not val:
            if i in [3]:
                val = " "
        vallist.append(val)
    # print (vallist)
    val=vallist[2]
    val=re.sub(r'[^\x00-\x7F]+', '', val)
    vallist[2]=val
    return vallist


def getvalulistsupplierprofiles(data, listkey):
    vallist = []
    for i in listkey:
        # index=listkey[i]
        if i not in [100,8]:
            val = data[i]
        elif i in [100]:
            val = " "
        elif i in [8]:
            val=data[i]
            checkfive=str(data[i])
            if not val:
                val=1
            if checkfive in str(5):
                val=2
        if not val:
            if i in [26, 27]:
                val = None
            elif i in [8]:
                val=0
            elif i in [5]:
                val=" "
            else:
                val = " "
        vallist.append(val)
    #print (vallist)
    return vallist

def getvalulistsuppliebussinesstype(data, listkey):
    vallist = []
    checkbstype=True
    for i in listkey:
        # index=listkey[i]
        val = data[i]
        if not val:
            if i in [12, 37]:
                val = str(0)
            elif i in [11]:
                checkbstype=False
            else:
                val = None
        vallist.append(val)
    # print (vallist)
    return vallist, checkbstype


def getprefmobile(data):
    vallist = []
    for i in range(len(data)):
        val = data[i]
        if i in [7]:
            val = str(data[7])
            if val:
                mobilelist = val.split("-")
                type(mobilelist)
                pref = mobilelist[0]
                mnumber = mobilelist[1]
                if 10 < len(mnumber):
                    mnumber = mnumber[0:10]
                vallist.append(pref)
                vallist.append(mnumber)
            else:
                vallist.append(" ")
                vallist.append(" ")
        if i in [7, 8]:
            pass
        elif val:
            vallist.append(val)
        else:
            if i in [5,9,10,11]:
                val = 0
            elif i in [12,13]:
                val=None
            else:
                val = " "
            vallist.append(val)
    # print (mobilelist)
    return vallist


def id_generator(size=32, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))