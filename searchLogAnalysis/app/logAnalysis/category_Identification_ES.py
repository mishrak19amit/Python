from elasticsearch import Elasticsearch


def get_elastic_instance():
    es = Elasticsearch([{'host': '35.154.255.26', 'port': 9200}])
    # es=Elasticsearch([{'host':'13.234.108.103','port':9201}])
    return es


def get_category_name(es_conn, category_code):
    cat_list = []
    res = es_conn.search(
        index='category_v4',
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "_id": """{anything}""".format(anything=category_code)
                            }
                        }
                    ]
                }
            }
        })
    for doc in res['hits']['hits']:
        # print("CategoryCode: %s CategoryName: %s" % (doc['_id'], doc['_source']['categoryName']))
        cat_list.append(doc['_source']['categoryName'])
    return cat_list[0]


def do_taxonomy_formatting(textonomy):
    if len(textonomy) == 0:
        return None
    else:
        last_textonomy = textonomy.pop().split("/")
        if len(last_textonomy) == 0:
            return None
        else:
            return last_textonomy
            # print(lastTestonomy[0])


def provide_category_hierarchy(es_conn, search_string):
    #print("Identifying category for CPN: [", cpn_desc, "]")
    res = es_conn.search(
        index='product_v2',
        body={
            "from": 0, "size": 1, "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": """{anything}""".format(anything=search_string),
                                "fields": [
                                    "categoryString.shingle_category^10.0",
                                    "productName.shingle_2^2.0"
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
        # print("%s %s" % (doc['_id'], doc['_source']['taxonomyList']))
        category_hierarchy = do_taxonomy_formatting(doc['_source']['taxonomyList'])
        return category_hierarchy


def category_for_search_string_by_es(es_conn, cpn_desc):
    category_hierarchy = provide_category_hierarchy(es_conn, cpn_desc)
    cat_dic = {}
    if category_hierarchy is not None:
        for cateCode in category_hierarchy:
            category_name = get_category_name(es_conn, cateCode)
            cat_dic[cateCode] = category_name
    return cat_dic


if __name__ == "__main__":
    print("Getting es connection..")
    es = get_elastic_instance()
    cpnDesc = "Greenleaf TA-02 28mm Round Tiller Attachment for Brush Cutter"
    print("es instance", es)
    catListES = category_for_search_string_by_es(es, cpnDesc)
    for data in catListES:
        print(catListES[data])


def get_category_from_es(text_df, es_conn):
    cat_level = []
    for cpn in text_df:
        cat_list = category_for_search_string_by_es(es_conn, cpn)
        cat_level.append(cat_list)
    return cat_level
