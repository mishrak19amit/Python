from elasticsearch import Elasticsearch
from app.logAnalysis.category_Identification_ES import category_for_search_string_by_es
from app.logAnalysis.brandIdentification_ES import brand_for_search_string_by_es
from app.logAnalysis.utility import stem_string


def get_elastic_instance():
    es = Elasticsearch([{'host': '35.154.255.26', 'port': 9200}])
    # es=Elasticsearch([{'host':'13.234.108.103','port':9201}])
    return es


def does_search_string_contain_brand(brand, search_string):
    stem_search_string = stem_string(search_string)
    brand = stem_string(brand)
    brand_lower_case = brand.lower()
    return brand_lower_case in stem_search_string


def get_brand_from_search_string(es_conn, search_string):
    brand_dic_es = brand_for_search_string_by_es(es_conn, search_string)
    brand_dic = {}
    for brand_id in brand_dic_es:
        if does_search_string_contain_brand(brand_dic_es[brand_id], search_string):
            brand_dic[brand_id] = brand_dic_es[brand_id]
            return brand_dic
        else:
            brand_dic[" "] = " "
    if not brand_dic:
        brand_dic[" "] = " "
    return brand_dic


def get_category_from_search_string(es_conn, search_string):
    cat_dic_es = category_for_search_string_by_es(es_conn, search_string)
    categories_dic = {}
    lst_category = ""
    for category_code in cat_dic_es:
        lst_category = category_code
    if lst_category is not "":
        categories_dic[lst_category]=cat_dic_es[lst_category]
    else:
        categories_dic[" "] = " "
    return categories_dic


def get_search_keyword_brand_category_list(es_conn, search_string):
    search_string_result_list = []
    cat_dic = get_category_from_search_string(es_conn, search_string)
    brand_dic = get_brand_from_search_string(es_conn, search_string)
    search_string_result_list.append(search_string)
    for cat_code in cat_dic:
        search_string_result_list.append(cat_code)
        search_string_result_list.append(cat_dic[cat_code])
    for brand_id in brand_dic:
        search_string_result_list.append(brand_id)
        search_string_result_list.append(brand_dic[brand_id])
    return search_string_result_list


if __name__ == "__main__":
    yes_or_no = does_search_string_contain_brand("Philips", "philips led bulb 5w")
    print(yes_or_no)
    print("Getting es connection..")
    es_conn = get_elastic_instance()
    # cpnDesc="I kall 7.1 Channel Black Bluetooth Multimedia Speakers, TA-777"
    # cpnDesc = "Kirloskar 1 Hp Openwell Submersible Pumps"
    cpnDesc = "vega motorcycle helmet"
    search_result = get_search_keyword_brand_category_list(es_conn, cpnDesc)
    print(search_result)
