from elasticsearch import Elasticsearch
import csv


def get_elastic_instance():
    es = Elasticsearch([{'host': '35.154.255.26', 'port': 9200}])
    # es=Elasticsearch([{'host':'13.234.108.103','port':9201}])
    return es


def brand_for_search_string_by_es(es_conn, cpn_desc):
    # print("Identifying brand for CPN: [", cpn_desc, "]")
    brand_dic = {}
    res = es_conn.search(
        index='product_v2',
        body={
            "from": 0, "size": 1, "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": """{anything}""".format(anything=cpn_desc),
                                "fields": [
                                    "brandName.shingle_2^10.0"
                                ],
                                "type": "best_fields",
                                "operator": "OR",
                                "slop": 0,
                                "prefix_length": 0,
                                "max_expansions": 50,
                                "tie_breaker": 1,
                                "zero_terms_query": "NONE",
                                "auto_generate_synonyms_phrase_query": True,
                                "fuzzy_transpositions": True,
                                "boost": 1
                            }
                        }
                    ],
                    "filter": [
                        {
                            "term": {
                                "isActive": {
                                    "value": True,
                                    "boost": 1
                                }
                            }
                        }
                    ],
                    "adjust_pure_negative": True,
                    "boost": 1
                }
            }
        }
    )

    for doc in res['hits']['hits']:
        #print("%s %s" % (doc['_id'], doc['_source']['brandName']))
        brand_id = doc['_source']['brandId']
        brand_name = doc['_source']['brandName']
        brand_dic[brand_id] = brand_name
    return brand_dic


if __name__ == "__main__":
    print("Getting es connection..")
    es_conn = get_elastic_instance()
    print("es instance", es_conn)
    cpn_desc = "safety shoes"
    brand_list = brand_for_search_string_by_es(es_conn, cpn_desc)
    for brand in brand_list:
        print(brand)


def get_brand_from_es(text_df, es_conn):
    brand_list = []
    for cpn in text_df:
        brand = brand_for_search_string_by_es(es_conn, cpn)
        if len(brand) != 0:
            brand_list.append(brand[0])
    return brand_list
