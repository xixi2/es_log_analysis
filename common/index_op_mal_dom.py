from elasticsearch import helpers, Elasticsearch

HOST = "10.1.1.205:9200"
MAL_DOMAIN_INDEX_NAME = "malicious-domains"
MAL_DOMAIN_DOC_TYPE = 'logs'


def get_all_domains(es, index_name, doc_type, query_body=None):
    domain_set = set()
    print(index_name)
    if es.indices.exists(index_name):
        if not query_body:
            query_body = {"query": {"match_all": {}}}
        gen = helpers.scan(es, index=index_name, doc_type=doc_type, query=query_body)
        for item in gen:
            item = item['_source']
            domain_name = item['domain']
            domain_set.add(domain_name)
        return list(domain_set), len(domain_set)


def set_mal_domain_index_params(query_body=None):
    index_name = MAL_DOMAIN_INDEX_NAME
    doc_type = MAL_DOMAIN_DOC_TYPE
    es = Elasticsearch(hosts=HOST)
    domain_list, total_len = get_all_domains(es, index_name, doc_type, query_body)
    return domain_list


def set_mal_domain_index_params1(func, query_body=None):
    index_name = MAL_DOMAIN_INDEX_NAME
    doc_type = MAL_DOMAIN_DOC_TYPE
    es = Elasticsearch(hosts=HOST)
    return func(es, index_name, doc_type, query_body)


def get_domains_with_type(es, index_name, doc_type, query_body=None):
    print(index_name)
    domain_dict = {}
    if es.indices.exists(index_name):
        if not query_body:
            query_body = {"query": {"match_all": {}}}
        gen = helpers.scan(es, index=index_name, doc_type=doc_type, query=query_body)
        for item in gen:
            item = item['_source']
            print(item)
            domain_name = item['domain']
            source = item["info"][0]["Source"]
            mal_type = item["info"][0]["Desc"]
            domain_dict[domain_name] = (source, mal_type)
        return domain_dict
