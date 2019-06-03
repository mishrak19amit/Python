1. Check for source and destination database and tables
2. Check for log file.
3. check for query/database in AppendToTable
4. Check for python virtual environment
5. Taking 75 minutes for 47888 records 
6. Create table schema and migrate table which should be same follow TableSchemaAndSame.txt
7. When all the data is processed then run below command to trim the data


UPDATE supplier_banks SET account_holder_name = TRIM(account_holder_name)
,account_number = TRIM(account_number),ifsc_code = TRIM(ifsc_code),bank_name = TRIM(bank_name);

UPDATE supplier_profile SET gst_no = TRIM(gst_no),trade_name = TRIM(trade_name);

UPDATE supplier_address SET address1 = TRIM(address1),address2 = TRIM(address2);

UPDATE suppliers SET entity_name = TRIM(entity_name),contact_name = TRIM(contact_name),phone = TRIM(phone);


8. Then login_details table should be imported in enterprise_catalog database and make the sql export
9. delete the login_details from datamigration database and then create mysqldump to import on production


10. If required

pip install virtualenv
sudo virtualenv moglix
virtualenv -p /usr/bin/python2.7 moglix
source moglix/bin/activate
jupyter notebook

mysql.connector,configparser  pip installation
sudo apt-get install python-pip python-dev libmysqlclient-dev
pip install mysql-connector-python
sudo pip install MySQL-python
sudo pip install configparser

