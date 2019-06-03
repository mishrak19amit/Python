import SQL_cursor
import re


def inserting_Supplier_Address(data, addresscur, supplieraddresstablename, destmydb, logger):
    checkempty = False
    inser_query = query = """INSERT INTO """ + supplieraddresstablename + """ (supplier_id, address1, address2, city, state, country, pincode, phone_prefix, phone, type, is_deleted, is_default, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s)"""
    vallist = []
    mylist = []
    get_query = """ select supplier_address.supplier_id, supplier_address.address_1, supplier_address.address_2, supplier_address.city, (SELECT name from supplier.ml_state where id_state=supplier_address.state ) as state, supplier_address.country, supplier_address.pincode, supplier_address.phone_mobile, supplier_address.phone_mobile, supplier_address_mapping.type, supplier_address.deleted, supplier_address_mapping.is_default, supplier_address.created_at, supplier_address.updated_at from supplier.supplier_address left join supplier.supplier_address_mapping 
        on supplier_address.address_id=supplier_address_mapping.address_id
        where supplier_address.supplier_id="""
    supid = data[0]
    get_query = get_query + """""" + str(
        supid) + """  ORDER BY  supplier_address_mapping.is_default DESC,supplier_address.created_at DESC"""
    # print (get_query)
    addresscur.execute(get_query)
    checklist = []
    verification_stats = 3
    check1 = False
    check2 = False
    check3 = False
    for row in addresscur:
        # print (row)
        type = row[9]
        # print (type)
        if type in [1, 2, 3, 4]:
            mylist.append(row)

    for dataadress in mylist:
        typeval = dataadress[9]
        defaultval = dataadress[11]
        # print (type(val1))
        if defaultval is 1 and typeval is 1:
            check1 = True
        if defaultval is 1 and typeval is 3:
            check2 = True
        if defaultval is 1 and typeval is 4:
            check3 = True
    if check1 and check2 and check3:
        verification_stats = 4
    else:
        # print ("Address is not complete for supplier_id: " + str(supid))
        logger.info("Address is not complete for supplier_id: " + str(supid))
    checktypelist = [False, True, True, True, True]
    marklimitfive = [5, 0, 0, 0, 0]
    ## getting latest type zero value
    typewiselatestdeafultzero = [True, True, True, True, True]
    valdefzerotype = {}
    count = 0
    finallist = []
    for rowlist in mylist:
        # print (row)
        vallist = SQL_cursor.getprefmobile(rowlist)
        finallist.append(vallist)
        # print (vallist)
        # print (vallist)
        type = vallist[9]
        isdef = vallist[11]
        if marklimitfive[type] < 5:
            marklimitfive[type] = marklimitfive[type] + 1
        if isdef is 0 and typewiselatestdeafultzero[type]:
            valdefzerotype[type] = vallist
            typewiselatestdeafultzero[type] = False
    # print (valdefzerotype)
    for vallist in finallist:
        # print (row)
        # vallist = SQL_cursor.getprefmobile(rowlist)
        # print (vallist)
        # print (vallist)
        type = vallist[9]
        isdef = vallist[11]
        if checktypelist[type]:
            if isdef is 1:
                checktypelist[type] = False
            if marklimitfive[type] is 1 and checktypelist[type]:
                checktypelist[type] = False
                vallist = valdefzerotype[type]
                vallist[11] = str(1)
            try:
                if 0 < marklimitfive[type]:
                    SQL_cursor.appendtotable(destmydb, inser_query, vallist, logger)
                    checkempty = True
                    marklimitfive[type] = marklimitfive[type] - 1
            except:
                print (" Data not get inserted")
                return False
        else:
            vallist[11] = str(0)
            try:
                if 0 < marklimitfive[type]:
                    SQL_cursor.appendtotable(destmydb, inser_query, vallist, logger)
                    checkempty = True
                    marklimitfive[type] = marklimitfive[type] - 1
            except:
                print (" Data not get inserted")
                return False
            count += 1
    del checklist[:]
    del mylist[:]
    del vallist[:]
    del checklist[:]
    logger.info(" Data is being inserted")
    if (verification_stats is 4) and checkempty:
        return True
    else:
        return False


def inserting_Supplier_Bussiness_Type(data, destmydb, suppliebussinesstypetablename, logger):
    checkempty = False
    inser_query = query = """INSERT INTO """ + suppliebussinesstypetablename + """ (supplier_id,business_type,shipment_mode,credit_term,created_at,updated_at) VALUES (%s, %s, %s, %s, %s, %s)"""
    listkey = [0, 11, 12, 37, 26, 27]
    vallist, checkbstype = SQL_cursor.getvalulistsuppliebussinesstype(data, listkey)
    bstype = vallist[1]
    spmntmode = vallist[2]
    if spmntmode is 1 and bstype is 2:
        vallist[2] = str(1)
    else:
        vallist[2] = str(2)
    if bstype is 3:
        logger.info("business type is 3 so inserting value two times for supplier_id: " + str(vallist[0]))
        count = 1
        for i in range(2):
            vallist[1] = str(count)
            if count is 2 and spmntmode is 1:
                vallist[2] = str(1)
            else:
                vallist[2] = str(2)
            try:
                SQL_cursor.appendtotable(destmydb, inser_query, vallist, logger)
                checkempty = True
                count = count + 1
            except Exception as e:
                logger.error(str(e))
                return False
    elif bstype in [1, 2]:
        try:
            SQL_cursor.appendtotable(destmydb, inser_query, vallist, logger)
            checkempty = True
        except Exception as e:
            logger.error(str(e))
            return False
    else:
        logger.info(" businesstype is null, no need to push data")
    del vallist[:]
    if checkempty and checkbstype:
        return True
    else:
        return False


def Inserting_Supplier_Banks(data, addresscur, supplierbankstablename, destmydb, logger):
    supid = data[0]
    checkempty = False
    inser_query_supplierBanks = query = """INSERT INTO """ + supplierbankstablename + """ (supplier_id, account_type, account_number, account_holder_name, ifsc_code, bank_name, city, branch, is_default, is_deleted, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"""
    get_query = """ select  bank_details.supplier_id, bank_details.account_type, bank_details.account_number, bank_details.account_holder_name, bank_details.ifsc_code,  bank_details.bank_name, bank_details.city, bank_details.branch, bank_details_mapping.is_default, bank_details.deleted, bank_details.created_at, bank_details.updated_at from supplier.bank_details
   left join supplier.bank_details_mapping
   on bank_details.id=bank_details_mapping.bankdetail_id
   WHERE bank_details.supplier_id="""
    resultcount_query = """select  count(bank_details.supplier_id) from supplier.bank_details
    inner join supplier.bank_details_mapping
    on bank_details.supplier_id=bank_details_mapping.supplier_id
    WHERE bank_details.supplier_id="""
    resultcount_query = resultcount_query + """""" + str(supid)
    get_query = get_query + """""" + str(supid) + """ ORDER BY bank_details.created_at DESC"""
    # print (get_query)
    rowcount = 0
    addresscur.execute(resultcount_query)
    for data in addresscur:
        rowcount = data[0]
    lastrow = 0
    addresscur.execute(get_query)
    vallist = []
    isdefaultinserted = True
    for row in addresscur:
        # vallist.append(row[0])
        count = 0
        for val in row:
            # print type(val)
            if not val:
                if count is 1:
                    vallist.append(0)
                elif count is 8:
                    vallist.append(0)
                elif count is 9:
                    vallist.append(0)
                elif count in [10, 11]:
                    vallist.append(None)
                else:
                    vallist.append(" ")
            else:
                if count in [2, 3, 5, 6, 7]:
                    val = re.sub(r'[^\x00-\x7F]+', '', val)
                if count in [4]:
                    val = re.sub(r'[^\x00-\x7F]+', '', val)
                    if len(val) > 20:
                        val = val[0:20]
                vallist.append(val)
            count += 1
        if vallist[8] is 1:
            if isdefaultinserted:
                try:
                    SQL_cursor.appendtotable(destmydb, inser_query_supplierBanks, vallist, logger)
                    isdefaultinserted = False
                    checkempty = True
                except Exception as e:
                    logger.error(str(e))
                    return False
            else:
                vallist[8] = 0
                try:
                    SQL_cursor.appendtotable(destmydb, inser_query_supplierBanks, vallist, logger)
                    checkempty = True
                except Exception as e:
                    logger.error(str(e))
                    return False
        else:
            if lastrow < rowcount:
                try:
                    SQL_cursor.appendtotable(destmydb, inser_query_supplierBanks, vallist, logger)
                    checkempty = True
                except Exception as e:
                    logger.error(str(e))
                    return False
            elif isdefaultinserted and (lastrow is rowcount):
                vallist[8] = 1
                try:
                    SQL_cursor.appendtotable(destmydb, inser_query_supplierBanks, vallist, logger)
                    checkempty = True
                except Exception as e:
                    logger.error(str(e))
                    return False
            else:
                try:
                    SQL_cursor.appendtotable(destmydb, inser_query_supplierBanks, vallist, logger)
                    checkempty = True
                except Exception as e:
                    logger.error(str(e))
                    return False
        del vallist[:]
        lastrow += 1
    logger.info(" Data is being inserted")
    if checkempty:
        return True
    else:
        return False


def Inserting_Suppliers_Documents(data, addresscur, supplierdocumentstablename, supplierdocumentsmapping, destmydb,
                                  awsbaseurl, dirpath, logger):
    # print (data)
    # print(type(data))
    # print (len(data))
    checkempty = False
    inser_query_Suppliers_Documents = """INSERT INTO """ + supplierdocumentstablename + """ (supplier_id,pan_card,gstin,cancelled_cheque,bank_statement,corporation_certificate,business_address,pickup_address,signature,created_at,updated_at) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)"""
    get_query = """ select supplier_documents.supplier_id, supplier_documents.pan_card, supplier_documents.gst_card,supplier_documents.cancelled_cheque,supplier_documents.bank_statement, supplier_documents.bank_statement, supplier_documents.business_address, supplier_documents.pickup_address,  supplier_documents.pickup_address, supplier_profile.created_at, supplier_profile.updated_at from supplier.supplier_documents
    inner join supplier.supplier_profile
    on supplier_profile.id=supplier_documents.supplier_id
    where supplier_documents.supplier_id="""
    supid = data[0]
    get_query = get_query + """""" + str(supid) + """ Order by supplier_profile.created_at ASC"""
    # print (get_query)
    addresscur.execute(get_query)
    vallist = []
    verification_stats = 5
    checkfirstiter = True
    for row in addresscur:
        # vallist.append(row[0])
        count = 0
        for val in row:
            # print type(val)
            if not val:
                if count in [5, 8]:
                    if checkfirstiter:
                        vallist.append(" ")
                elif count in [9, 10]:
                    if checkfirstiter:
                        vallist.append(None)
                else:
                    if checkfirstiter:
                        vallist.append(" ")
            else:
                if count in [1, 2, 3, 4, 5, 6, 7, 8]:
                    if checkfirstiter:
                        vallist.append(val)
                    else:
                        vallist[count] = val
                else:
                    if checkfirstiter:
                        vallist.append(val)
            count += 1
        checkfirstiter = False
    if len(vallist) >= 8:
        if vallist[1] and vallist[2] and vallist[3] and vallist[4] and vallist[6] and vallist[7]:
            verification_stats = 10
    # Start inserting documents details in Document mapping
    if len(vallist) is 11:
        supdocid = vallist[0]
        credate = vallist[9]
        update = vallist[10]
        # Removing special character from filename
        for i in range(len(vallist)):
            if i in [1, 2, 3, 4, 5, 6, 7, 8]:
                if vallist[i] is not " ":
                    key = SQL_cursor.id_generator();
                    key = key.lower()
                    filename = vallist[i]
                    filename = re.sub(r'[^a-zA-Z0-9._-]', "", filename)
                    filename = filename.lower()
                    Inserting_Suppliers_Documents_Mapping(destmydb, supdocid, filename, credate, update, key,
                                                          awsbaseurl,
                                                          supplierdocumentsmapping, dirpath, logger)
                    vallist[i] = key
            # Inserting into document table
        try:
            SQL_cursor.appendtotable(destmydb, inser_query_Suppliers_Documents, vallist, logger)
            checkempty = True
        except Exception as e:
            logger.error(str(e))
            return False
        del vallist[:]
        logger.info(" Data has been inserted")
    if verification_stats is 10 and checkempty:
        return True
    else:
        return False


def Inserting_Suppliers_Documents_Mapping(destmydb, supid, doc, creatat, updateat, key, awsbaseurl,
                                          supplierdocumentsmapping, dirpath, logger):
    insert_query = """ INSERT INTO  """ + supplierdocumentsmapping + """ (supplier_id, file_key, file_url, file_name, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"""
    dirpath = dirpath + str(supid)
    filename = dirpath + "/" + doc
    fileurl = awsbaseurl + filename
    vallist = []
    vallist.append(supid)
    vallist.append(key)
    vallist.append(fileurl)
    vallist.append(filename)
    vallist.append(creatat)
    vallist.append(updateat)
    try:
        SQL_cursor.appendtotable(destmydb, insert_query, vallist, logger)
    except Exception as e:
        logger.error(str(e))
        return False
    del vallist[:]


def InsertUserAuth(destmydb, vallist, userauthtable, logger):
    insert_query_userAuth = """ INSERT INTO  """ + userauthtable + """ (account_enable, account_lock, domain_type, email, email_verified, mobile_number, mobile_verified, password, user_role, user_id, user_name)  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    userauthvallist = []
    userauthvallist.append(1)
    userauthvallist.append(0)
    userauthvallist.append('SC')
    userauthvallist.append(vallist[4])
    userauthvallist.append(1)
    userauthvallist.append(vallist[6])
    userauthvallist.append(1)
    userauthvallist.append(vallist[5])
    if vallist[10] is not 0:
        userauthvallist.append("ADMIN")
    else:
        userauthvallist.append("SUPPLIER")
    userauthvallist.append(vallist[0])
    userauthvallist.append(vallist[3])
    try:
        SQL_cursor.appendtotable(destmydb, insert_query_userAuth, userauthvallist, logger)
    except Exception as e:
        logger.error(str(e))
        return False
    del vallist[:]
