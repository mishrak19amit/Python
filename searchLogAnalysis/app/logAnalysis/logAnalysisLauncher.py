from app.logAnalysis.mySQLDao import get_row_count_count, get_batch_result_set, get_last_batch_result_set, save_to_mysql, clean_mysql_table
import pandas as pd
from app.logAnalysis.utility import get_logger, get_batch_size
from app.logAnalysis.searchKeywordAnalysis import get_elastic_instance, get_search_keyword_brand_category_list

logger = get_logger()
batch_size = get_batch_size()


def prepare_df_save_mysql(result_list):
    try:
        result_df = pd.DataFrame.from_records(result_list,
                                              columns=['searchString', 'category_code', 'category_name', 'brand_id',
                                                       'brand_name', 'search_count'])
        save_to_mysql(result_df)
    except Exception as e:
        print(e)


def process_mysql_result(es_conn, result):
    result_list=[]
    for data in result:
        print(data)
        search_result_list=get_search_keyword_brand_category_list(es_conn, data[0])
        search_result_list.append(data[1])
        result_list.append(search_result_list)
    prepare_df_save_mysql(result_list)


def start_launcher():
    clean_mysql_table()
    es_conn=get_elastic_instance()
    row_count = get_row_count_count()
    batch_count = row_count / int(batch_size)
    batch_count = int(batch_count)
    batch_num = 0
    for batch_num in range(batch_count):
        print("Going for batch number ..."+ str(batch_num +1))
        result = get_batch_result_set(batch_num)
        process_mysql_result(es_conn, result)
    batch_count += 1
    print("Going for last batch ..." + str(batch_num+1))
    result = get_last_batch_result_set(batch_num, row_count)
    process_mysql_result(es_conn, result)


if __name__ == "__main__":
    start_launcher()
    # es_conn= get_elastic_instance()
    # res=get_search_keyword_brand_category_list(es_conn, 'gas  stove')
    # print(res)
