import redis
import time
from pymongo import MongoClient
from common.mongodb_op import mongo_url
from common.common_niclog_url import DOMAIN_URL_VISITING, GOOD_DOMAIN_URL_VISITING, DOMAIN_BASIC_COL, DOMAIN_IP_COL, \
    DOMAIN2IP_COL
from common.mongo_common import DOMAIN_2ND_FIELD, SUBDOMAINS_FIELD, IP_FIELD

redis_host = "127.0.0.1"
r0 = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
r1 = redis.Redis(host=redis_host, port=6379, db=1, decode_responses=True)
client = MongoClient(mongo_url)
db_good_basic = client[GOOD_DOMAIN_URL_VISITING]
db_basic = client[DOMAIN_URL_VISITING]
db_dict = {
    0: db_good_basic,
    1: db_basic
}
col_basic = DOMAIN_BASIC_COL
col_ip_basic = DOMAIN_IP_COL
col_domain2ip = DOMAIN2IP_COL


# def save_key_set2mongodb(key, values):
#     """
#     指定一个键，将一个集合中的所有元素逐一加入到这个键的值中
#     :param key:
#     :param values:
#     :return:
#     """
#     query_body = {"domain": domain}
#     basic_body = {"$addToSet": {"subdomains": {"$each": subdomains}}}
#     db[mongo_index].update(query_body, basic_body, True)


def save_redis2mongodb(domain_bad):
    """
    每隔interval时间将redis中的内容插入到MongoDB中
    :return:
    从redis读出的键值对一开始是二进制形式，需要decode成utf8形式
    """
    r = r1 if domain_bad else r0
    db = db_dict[domain_bad]
    keys = r.keys()  # list
    if not keys:
        return "empty redis"
    domains = set(key[:-3] for key in keys if key.endswith("_ip"))
    for domain in domains:
        domain_ip_key = domain + "_ip"
        domain_sub_key = domain + "_sub"
        domain_ip = r.smembers(domain_ip_key)
        domain_sub = r.smembers(domain_sub_key)
        r.delete(domain_ip_key)
        r.delete(domain_sub_key)
        sub_domains = [sub + "." + domain for sub in domain_sub]
        print("domain_ip: %s, domain_sub: %s" % (domain_ip, sub_domains))
        query_body = {DOMAIN_2ND_FIELD: domain}
        basic_body = {
            "$addToSet": {SUBDOMAINS_FIELD: {"$each": list(sub_domains)}, IP_FIELD: {"$each": list(domain_ip)}}}
        db[col_basic].update(query_body, basic_body, True)

    ip_domain_keys = set(key[3:] for key in keys if key.startswith("ip_"))
    for ip in ip_domain_keys:
        ip_domain_key = "ip_" + ip
        ip_domains = r.smembers(ip_domain_key)
        r.delete(ip_domain_key)

        # print("key:%s, ip_domain: %s" % (ip, ip_domains))
        ip_query_body = {IP_FIELD: ip}
        ip_basic_body = {"$addToSet": {DOMAIN_2ND_FIELD: {"$each": list(ip_domains)}}}
        db[col_ip_basic].update(ip_query_body, ip_basic_body, True)


def repeat_check_redis_for_query(domain_bad):
    continous_miss = 0  # 连续检查redis都为空的次数
    while True:
        time.sleep(60)
        if continous_miss > 10:
            break
        res = save_redis2mongodb(domain_bad)
        if res:
            print(res)
            continous_miss += 1
        else:
            continous_miss = 0


if __name__ == '__main__':
    domain_bad = 0
    repeat_check_redis_for_query(domain_bad)
