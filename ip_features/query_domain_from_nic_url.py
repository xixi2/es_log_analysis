"""
读取niclog_url文件，取出其中的域名，匹配恶意域名数据集2，并将相关信息存入数据库
"""
import os
import sys
import json
import time

from threading import Thread
from pymongo import MongoClient
from common.mongodb_op import mongo_url
from common.common_niclog_url import NICLOG_URL_FILE_DIR, FILE_NAME_SUFFIX, FILE_NAME_PREFIX, HOST_NAME, DEST_IP, APPID, \
    GEO_DEST_IP_IP, GEO_DEST_IP_COUNTRY, GEO_DEST_IP_PROVINCE, GEO_DEST_IP_CITY, TIME_STAMP, DOMAIN_URL_VISITING, \
    GOOD_DOMAIN_URL_VISITING, DOMAIN_BASIC_COL, DOMAIN_IP_COL, DOMAIN_VISITOR_COL, DOMAIN2IP_COL
from common.mongo_common import DOMAIN_2ND_FIELD, SUBDOMAINS_FIELD, IP_FIELD
from common.date_op import generate_day_seq
from common.domains_op import keep_2nd_dom_name, keep_only_3th_dom_name, is_domain_ip, read_domain_file, write2file
from common.other_common import remove_file
from common.mongodb_op import get_all_domains
from common.url_log_domain_common import GOOD_URL_DOMAINS_FILE, BAD_URL_DOMAINS_FILE, START_DAY, FILE_SEQ
from ip_features.trie_tree import Trie
import redis

redis_host = "127.0.0.1"
r0 = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
r1 = redis.Redis(host=redis_host, port=6379, db=1, decode_responses=True)
client = MongoClient(mongo_url)
db_good_basic = client[GOOD_DOMAIN_URL_VISITING]
db_basic = client[DOMAIN_URL_VISITING]
col_basic = DOMAIN_BASIC_COL
col_ip_basic = DOMAIN_IP_COL
col_domain2ip = DOMAIN2IP_COL
db_dict = {
    0: db_good_basic,
    1: db_basic
}


def load_good_domains(domain_bad, num):
    domain_list = get_all_domains(client, domain_bad)
    print("len of domain_list: %s" % (len(domain_list)))
    if num < len(domain_list):
        domain_list = domain_list[:num]
    print("len of domain_list: %s" % (len(domain_list)))
    return domain_list


def tackle_line(domain):
    domain_2nd = ""
    if domain and not is_domain_ip(domain):
        domain_2nd = keep_2nd_dom_name(domain)
    return domain_2nd


def split_url_log_line(line):
    """将niclog_url日志的一行进行切分"""
    line = line.strip("\n")
    d = json.loads(line)
    # print(d)

    domain = d.get(HOST_NAME, "")
    domain_2nd = tackle_line(domain)
    ip_belonging_to = d.get(DEST_IP, "")
    app_type = d.get(APPID, "")
    geo_ip = d.get(GEO_DEST_IP_IP, "")
    geo_ip_country = d.get(GEO_DEST_IP_COUNTRY, "")
    geo_ip_province = d.get(GEO_DEST_IP_PROVINCE, "")
    geo_ip_city = d.get(GEO_DEST_IP_CITY, "")
    time_stamp = d.get(TIME_STAMP, "")

    if domain_2nd:
        sub_domain = keep_only_3th_dom_name(domain)
        # print(
        #     "domain: %s, sub_domain: %s, ip_belonging_to: %s, app_type: %s, geo_ip: %s, "
        #     "geo_ip_country: %s, geo_ip_province: %s, geo_ip_city:%s" % (
        #         domain_2nd, sub_domain, ip_belonging_to, app_type, geo_ip, geo_ip_country, geo_ip_province,
        #         geo_ip_city))
        return (domain_2nd, sub_domain, ip_belonging_to, app_type, geo_ip, geo_ip_country, geo_ip_province,
                geo_ip_city, time_stamp)


def save_domain_info2mongodb(records, domain_bad):
    db = db_dict[domain_bad]
    for record in records:
        domain_2nd, sub_domain, ip_belonging_to, app_type, geo_ip, geo_ip_country, geo_ip_province, geo_ip_city, _ = record
        query_body = {DOMAIN_2ND_FIELD: domain_2nd}
        # print("domain: %s, sub_domain: %s, ip_belonging_to: %s" % (domain_2nd, sub_domain, ip_belonging_to))

        # 添加域名基本信息
        basic_body = {}
        if ip_belonging_to:
            basic_body["$addToSet"] = {IP_FIELD: ip_belonging_to}
        if sub_domain:
            if not basic_body["$addToSet"]:
                basic_body["$addToSet"] = {}
            basic_body["$addToSet"][SUBDOMAINS_FIELD] = sub_domain
        if basic_body:
            # print("92 basic_body: %s" % basic_body)
            db[col_basic].update(query_body, basic_body, True)

        # 这里添加appid到数据库中，暂时不添加
        # app_body = {APPID: app_type}

        # 添加访问者信息

        # 添加ip信息之前必须保证ip不为空
        if not ip_belonging_to:
            continue

        # 添加ip信息
        ip_query_body = {IP_FIELD: ip_belonging_to}
        ip_basic_body = {}
        if geo_ip_country: ip_basic_body[GEO_DEST_IP_COUNTRY] = geo_ip_country
        if geo_ip_province: ip_basic_body[GEO_DEST_IP_PROVINCE] = geo_ip_province
        if geo_ip_city: ip_basic_body[GEO_DEST_IP_CITY] = geo_ip_city

        if ip_basic_body:
            db[col_ip_basic].update(ip_query_body, ip_basic_body, True)
        else:
            db[col_ip_basic].insert(ip_query_body)

        # 添加ip对应的域名信息
        query_body = {IP_FIELD: ip_belonging_to}
        basic_body = {"$addToSet": {DOMAIN_2ND_FIELD: domain_2nd}}
        # print("basic_body: %s" % basic_body)
        db[col_domain2ip].update(query_body, basic_body, True)


def load_queried_domains(domain_bad):
    """从db_basic中找出已经访问过的域名，这是为了将那些没有访问过的域名排除出去，以便所有匹配的时间"""
    db = db_dict[domain_bad]
    recs = db[col_basic].find()
    domain_set = set()
    for rec in recs:
        domain_2nd = rec[DOMAIN_2ND_FIELD]
        domain_set.add(domain_2nd)
    return domain_set


def delete_not_visited_domains():
    # =================================================================================
    # 因为600个域名中除了自己找到的中文流行域名之后，还有许多国外域名从未被访问，匹配时间过长，因为删除这些从未被访问的域名
    domain_set = load_queried_domains(domain_bad)
    domain_exist = set(domain_list) & domain_set
    print("len of domain_exist: %s" % (len(domain_exist)))
    for domain in domain_exist:
        print("domain_2nd:", domain)
    remove_file(GOOD_URL_DOMAINS_FILE) if not domain_bad else remove_file(BAD_URL_DOMAINS_FILE)
    write2file(GOOD_URL_DOMAINS_FILE, domain_exist) if not domain_bad else write2file(BAD_URL_DOMAINS_FILE,
                                                                                      domain_exist)
    # =================================================================================


# def read_niclog_url_file(file, domains, domain_bad):
#     batch_num = 50
#     f_out = open(file)
#     I = iter(f_out)
#     file_total_line = 0
#     records = []
#     while True:
#         try:
#             file_total_line += 1
#             line = next(I)
#             record = split_url_log_line(line)
#             if record:
#                 if record[0] in domains:
#                     records.append(list(record))
#                     if len(records) >= batch_num:
#                         save_domain_info2mongodb(records, domain_bad)
#                         records = []
#         except StopIteration as e:
#             # print("StopIteration %s" % (e))
#             if len(records):
#                 save_domain_info2mongodb(records, domain_bad)
#             break
#         except Exception as e:
#             # print("error read file %s for %s" % (file, e))
#             pass
#     print("file %s totally has %s lines" % (file, file_total_line))


# def read_niclog_url_file(file, trie, domain_bad):
#     batch_num = 500
#     f_out = open(file)
#     I = iter(f_out)
#     file_total_line = 0
#     records = []
#     while True:
#         try:
#             if file_total_line % 500000 == 0:
#                 print("total_lines: %s" % (file_total_line))
#             file_total_line += 1
#             line = next(I)
#             record = split_url_log_line(line)
#             exists = trie.search(record[0])
#             # print("domain_2nd: %s, exists: %s" % (record[0], exists))
#             if record and exists:
#                 records.append(list(record))
#                 if len(records) >= batch_num:
#                     save_domain_info2mongodb(records, domain_bad)
#                     print("save_domain_info2mongodb")
#                     records = []
#         except StopIteration as e:
#             # print("StopIteration %s" % (e))
#             if len(records):
#                 save_domain_info2mongodb(records, domain_bad)
#             break
#         except Exception as e:
#             # print("error read file %s for %s" % (file, e))
#             pass
#     print("file %s totally has %s lines" % (file, file_total_line))

def save_domain_info2redis(record, domain_bad):
    """把每一条域名信息存入到redis数据库中"""
    domain_2nd, sub_domain, ip_belonging_to, app_type, geo_ip, geo_ip_country, geo_ip_province, geo_ip_city, _ = record
    # print("domain: %s, sub_domain: %s, ip_belonging_to: %s" % (domain_2nd, sub_domain, ip_belonging_to))
    r = r1 if domain_bad else r0

    # 添加域名基本信息
    if ip_belonging_to:
        domain_ip_key = domain_2nd + "_ip"
        r.sadd(domain_ip_key, ip_belonging_to)

        # 添加ip对应的域名信息
        ip_domain_key = "ip_" + ip_belonging_to
        r.sadd(ip_domain_key, domain_2nd)
        # print("domain: %s, ip: %s" % (domain_2nd, ip_belonging_to))
    if sub_domain:
        domain_sub_key = domain_2nd + "_sub"
        r.sadd(domain_sub_key, sub_domain)

    # 这里添加appid到数据库中，暂时不添加
    # 添加访问者信息


def read_niclog_url_file(file, trie, domain_bad):
    f_out = open(file)
    I = iter(f_out)
    file_total_line = 0
    while True:
        try:
            # if file_total_line % 500000 == 0:
            #     print("total_lines: %s" % (file_total_line))
            file_total_line += 1
            line = next(I)
            record = split_url_log_line(line)
            exists = trie.search(record[0])
            if record and exists:
                save_domain_info2redis(record, domain_bad)
        except StopIteration as e:
            # print("StopIteration %s" % (e))
            break
        except Exception as e:
            # print("error read file %s for %s" % (file, e))
            pass
    print("file %s totally has %s lines" % (file, file_total_line))


def get_niclog_url_file_list(dir=NICLOG_URL_FILE_DIR):
    file_list = []
    dt_str_seq = generate_day_seq(START_DAY, 3)
    for dt_str in dt_str_seq:
        for i in range(FILE_SEQ):
            file = dir + FILE_NAME_PREFIX + dt_str + "_" + str(i) + FILE_NAME_SUFFIX
            if not os.path.exists(file):
                # print("file: %s not exist" % file)
                continue
            file_list.append(file)
    return file_list


def read_niclog_url_files(file_list, domains, domain_bad):
    trie = build_trie_tree(domains)
    print("len of file_list: %s" % len(file_list))
    for file in file_list:
        start_time = time.time()
        print("read_niclog_url_file: %s" % file)
        read_niclog_url_file(file, trie, domain_bad)
        # read_niclog_url_file(file, domains, domain_bad)
        end_time = time.time()
        cost_time = end_time - start_time
        print("cost_time: %s 秒" % (cost_time))


def build_trie_tree(domains):
    trie = Trie()
    for domain in domains:
        trie.insert(domain)
    return trie


def save_redis2mongodb(domain_bad, interval=60):
    """
    每隔interval时间将redis中的内容插入到MongoDB中
    :param interval:
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
        # print("domain_ip: %s, domain_sub: %s" % (domain_ip, sub_domains))
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
    # ====================第一次随机加载正常域名，此后使用下句========================
    # good_domains = load_good_domains(0, 400)  # domain_bad:0, num: 400
    # write2file(GOOD_URL_DOMAINS_FILE, good_domains)
    # ================================================================================

    domain_bad = 0
    bad_domains = read_domain_file(BAD_URL_DOMAINS_FILE)  # 加载已经从niclog_url日志中查询到的恶意域名
    good_domains = read_domain_file(GOOD_URL_DOMAINS_FILE)  # 加载已经从niclog_url日志中查询到的正常域名
    domain_list = bad_domains if domain_bad else good_domains

    print("len of domain_list: %s" % (len(domain_list)))
    # 读取文件，从niclog_url日志中匹配恶意域名访问记录
    file_list = get_niclog_url_file_list()
    # read_niclog_url_files(file_list, domain_list, domain_bad)

    # 创建两个线程,t1负责读取
    t1 = Thread(target=read_niclog_url_files, args=(file_list, domain_list, domain_bad))  # args是要传入方法的参数
    t2 = Thread(target=repeat_check_redis_for_query, args=(domain_bad,))

    t1.start()
    t2.start()
