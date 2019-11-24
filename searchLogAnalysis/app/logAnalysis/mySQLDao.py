import mysql.connector
import configparser
from sqlalchemy import create_engine
from app.logAnalysis.utility import get_logger

config = configparser.ConfigParser()
config.read('config/config.ini', encoding='utf-8')
size = config['BATCH_DATA_SIZE']['row_size']
print("Batch size: " + str(size))
sql_hostname = config['DATA_SOURCE']['hostname']
sql_username = config['DATA_SOURCE']['username']
sql_password = config['DATA_SOURCE']['password']
sql_db_name = config['DATA_SOURCE']['db_name']
sql_table_name = config['DATA_SOURCE']['table_name']
search_analysis_table = config['DATA_SOURCE']['search_analysis_table']

logger = get_logger()


def get_source_db():
    source_db = mysql.connector.connect(
        host=sql_hostname,
        user=sql_username,
        passwd=sql_password,
        database=sql_db_name,
        buffered=True
    )
    return source_db


def check_table_exists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False


def clean_mysql_table():
    source_db = get_source_db()
    if check_table_exists(source_db, search_analysis_table):
        mycursor = source_db.cursor()
        mycursor.execute("truncate table " + search_analysis_table)
        print(search_analysis_table + " truncated successfully")


def save_to_mysql(datasetDF):
    engine = create_engine("mysql+pymysql://root:moglix@localhost/searchAnalysis"
                           .format(user="root",
                                   pw="moglix",
                                   db="pandas"))
    datasetDF.to_sql(con=engine, name='searckStringCategoryBrand', if_exists='append')


def get_row_count_count():
    source_db_instance = get_source_db()
    count_cur = source_db_instance.cursor()
    count_cur.execute("select count(*) as num from " + sql_table_name)
    count = 0
    for x in count_cur:
        logger.info("total records in table: " + str(x[0]))
        count = x[0]
    return count


def get_batch_result_set(batch_num):
    query = "select * from " + sql_table_name + " limit  " + str(batch_num * int(size)) + ", " + str(int(size))
    source_db_instance = get_source_db()
    count_cur = source_db_instance.cursor()
    count_cur.execute(query)
    result = count_cur.fetchall()
    return result


def get_last_batch_result_set(batch_num, row_count):
    query = "select * from " + sql_table_name + " limit  " + str(batch_num * int(size)) + ", " + str(
        row_count % int(size))
    source_db_instance = get_source_db()
    count_cur = source_db_instance.cursor()
    count_cur.execute(query)
    result = count_cur.fetchall()
    return result
