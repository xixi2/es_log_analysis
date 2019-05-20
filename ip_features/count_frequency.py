import time
import redis
from threading import Thread
from pymongo import MongoClient
from common.mongodb_op import mongo_url
from common.date_op import timestamp_str2ymdh
from ip_features.query_domain_from_nic_url import split_url_log_line, get_niclog_url_file_list, load_good_domains
from ip_features.query_domain_from_nic_url import BAD_URL_DOMAINS_FILE, GOOD_URL_DOMAINS_FILE
from common.mongo_common import DOMAIN_2ND_FIELD, SUBDOMAINS_FIELD, IP_FIELD, DATE_FIELD
from common.common_niclog_url import DOMAIN_URL_VISITING, GOOD_DOMAIN_URL_VISITING, DOMAIN_FREQUENCY_COUNTING_COL, \
    DOMAIN_BASIC_COL
from common.domains_op import read_domain_file, write2file
from ip_features.query_domain_from_nic_url import build_trie_tree

redis_host = "127.0.0.1"
r0 = redis.Redis(host=redis_host, port=6379, db=3, decode_responses=True)
r1 = redis.Redis(host=redis_host, port=6379, db=4, decode_responses=True)
client = MongoClient(mongo_url)
db_basic = client[DOMAIN_URL_VISITING]
db_good_basic = client[GOOD_DOMAIN_URL_VISITING]
col_frequency = DOMAIN_FREQUENCY_COUNTING_COL
db_dict = {
    0: db_good_basic,
    1: db_basic
}


def count_from_redis2mongodb(domain_bad):
    """
    将保存在redis中的访问次数信息存入mongodb数据库中
    :param domain_bad:
    :return:
    """
    r = r1 if domain_bad else r0
    keys = r.keys()
    if not keys:
        return "empty redis"
    for key in keys:
        count = r.get(key)
        r.delete(key)
        try:
            count = int(count)
        except Exception as e:
            print("error: %s" % (e))
        key_list = key.split("_")
        # print("type of key: %s, key: %s, key_list: %s" % (type(key), key, key_list))
        domain_2nd = key_list[0]
        dt_str_day = key_list[1]
        index = key_list[2]
        db = db_dict[domain_bad]
        query_body = {DOMAIN_2ND_FIELD: domain_2nd, DATE_FIELD: dt_str_day}
        # basic_body = {"$set": {index: count}}
        # print("query_body: %s, basic_body: %s" % (query_body, basic_body))
        # db[col_frequency].update(query_body, basic_body, True)
        rec = db[col_frequency].find(query_body)
        if not rec.count():
            basic_body = {"$set": {index: count}}
        else:
            # old_count = rec[0].get(index, 0)
            basic_body = {"$inc": {index: count}}
            # print("index: ", index, " rec[0]:", rec[0], " old_count: ", old_count, " count: ", count)
        db[col_frequency].update(query_body, basic_body, True)


def read_niclog_url_file(file, trie, domain_bad):
    """
    通过比较domains中的域名进行匹配，统计每个域名的访问次数
    :param file:
    :param domains:
    :return:
    """
    r = r1 if domain_bad else r0

    f_out = open(file)
    I = iter(f_out)
    file_total_line = 0
    while True:
        try:
            file_total_line += 1
            line = next(I)
            record = split_url_log_line(line)
            domain_2nd = record[0]
            timestamp = record[-1]
            exists = trie.search(domain_2nd)

            if exists and timestamp:
                dt_str = timestamp_str2ymdh(timestamp)
                index, dt_str_day = dt_str[-2:], dt_str[:-2]
                count_key = domain_2nd + "_" + dt_str_day + "_" + index
                r.incr(count_key)
                # print("domain_2nd: %s, exists: %s" % (domain_2nd, exists))
        except StopIteration as e:
            # print("StopIteration %s" % (e))
            break
        except Exception as e:
            # print("error read file %s for %s" % (file, e))
            pass
    print("file %s totally has %s lines" % (file, file_total_line))


def count_domains_queries(domains, file_list, domain_bad):
    print("====================建立字典树=======================")
    start_time = time.time()
    trie = build_trie_tree(domains)
    end_time = time.time()
    cost_time = end_time - start_time
    print("cost_time: %s 秒" % (cost_time))
    print("===========================================")

    print("len of file_list: %s" % len(file_list))
    for file in file_list:
        start_time = time.time()
        print("count_domains_queries: %s" % (file,))
        read_niclog_url_file(file, trie, domain_bad)
        end_time = time.time()
        cost_time = end_time - start_time
        print("file: %scost_time: %s 秒" % (file, cost_time))


def get_good_niclog_domain():
    """
    将在niclog中访问过的正常域名提取出来
    :return:
    """
    recs = db_basic[DOMAIN_BASIC_COL].find()
    domain_set = set()
    for rec in recs:
        domain = rec[DOMAIN_2ND_FIELD]
        domain_set.add(domain)
    print("len of domain_set: %s" % (len(domain_set)))
    write2file("good_niclog_url.txt", domain_set)


def repeat_check_redis_for_count(domain_bad):
    """
    每一分钟检查依次redis，若redis不为空将其中的数据存入到MongoDB中，否则将continous_miss加1(continous_miss表示连续检查redis都为空的次数)
    若continous_miss大于10（自定义的，个人认为不会出现10分钟内都没有数据存入redis的情况），则认为query_domain_from_nic已经停止运行，则退出此程序
    :param
    domain_bad:
    :return:
    """
    continous_miss = 0  # 连续检查redis都为空的次数
    while True:
        time.sleep(5)
        if continous_miss > 10:
            break
        res = count_from_redis2mongodb(domain_bad)
        if res:
            print(res)
            continous_miss += 1
        else:
            continous_miss = 0


if __name__ == '__main__':
    domain_bad = 0
    # 加载已经从niclog_url日志中查询到的恶意域名
    bad_url_domains = read_domain_file(BAD_URL_DOMAINS_FILE)
    good_url_domains = read_domain_file(GOOD_URL_DOMAINS_FILE)  # 加载已经从niclog_url日志中查询到的正常域名
    domains = bad_url_domains if domain_bad else good_url_domains

    print("len of domain_list: %s" % (len(domains)))

    # 读取文件，从niclog_url日志中匹配恶意/正常域名访问记录, 统计恶意/正常域名的访问次数
    file_list = get_niclog_url_file_list()
    # count_domains_queries(domains, file_list, domain_bad)

    # 创建两个线程,t1负责读取
    t1 = Thread(target=count_domains_queries, args=(domains, file_list, domain_bad))  # args是要传入方法的参数
    t2 = Thread(target=repeat_check_redis_for_count, args=(domain_bad,))

    t1.start()
    t2.start()
