import time
import math

import pandas as pd
from pymongo import MongoClient
from common.mongodb_op import mongo_url
from common.domains_op import read_domain_file
from ip_features.query_domain_from_nic_url import BAD_URL_DOMAINS_FILE, GOOD_URL_DOMAINS_FILE
from ip_features.query_domain_from_nic_url import load_good_domains
from common.mongo_common import IP_FIELD, DOMAIN_2ND_FIELD, SUBDOMAINS_FIELD
from ip_features.query_domain_from_nic_url import BAD_URL_DOMAINS_FILE, GOOD_URL_DOMAINS_FILE
from common.domains_op import read_domain_file
from common.common_niclog_url import DOMAIN_URL_VISITING, GOOD_DOMAIN_URL_VISITING, DOMAIN_BASIC_COL, DOMAIN2IP_COL
from common.file_names import IP_FEATURE_FILE

client = MongoClient(mongo_url)
db_bad = client[DOMAIN_URL_VISITING]
db_good = client[GOOD_DOMAIN_URL_VISITING]

NUMBER_OF_UNIQUE_IPS = "ip_count"  # 该域名可以映射到的ip个数
NUMBER_OF_UNIQUE_SUBDOMAINS = "subdomain_count"
NUMBER_OF_DOMAINS = "number_of_domains"  # 与该域名共享ip的域名个数
IP_ENTROPY = "ip_entropy"  # ip 信息熵
SUBDOMAIN_ENTROPY = "subdomain_entropy"  # 子域名信息熵

domain_mongo_index = DOMAIN_BASIC_COL
ip_mongo_index = DOMAIN2IP_COL

client = MongoClient(mongo_url)


def cut_ip_with_mask(ip, mask_len):
    ip_list = ip.split(".")
    ip_prefix = ".".join(ip_list[:mask_len])
    # print("ip_prefix: %s" % ip_prefix)
    return ip_prefix


def cal_entropy(d, card):
    """
    d一个字典，值表示键key的元素在集合中的个数
    :param d:
    :param card: 元集合中总的元素个数
    :return:
    """
    sum = 0.0
    for key in d:
        p_x = d[key] / card
        sum = sum - p_x * math.log(p_x, 2)
    entropy = sum / math.log(card, 2)
    return entropy


def cal_ip_entropy(ips, mask_len=16):
    """计算域名的ip_entropy:域名的ip熵值"""
    ip_card = len(ips)
    if ip_card == 0 or ip_card == 1:
        return 0.0
    mask_len = mask_len // 8
    ip_dict = {}
    for ip in ips:
        ip_prefix = cut_ip_with_mask(ip, mask_len)
        ip_dict.setdefault(ip_prefix, 0)
        ip_dict[ip_prefix] += 1
    ip_entropy = cal_entropy(ip_dict, ip_card)
    return ip_entropy


def cal_subdomain_entropy(subdomains):
    """计算域名的子域名熵值"""
    subdomain_card = len(subdomains)
    if subdomain_card == 0 or subdomain_card == 1:
        return 0.0
    subdomain_dict = {}
    for subdomain in subdomains:
        len_of_subdomain = len(subdomain)
        subdomain_dict.setdefault(len_of_subdomain, 0)
        subdomain_dict[len_of_subdomain] += 1
    subdomain_entropy = cal_entropy(subdomain_dict, subdomain_card)
    return subdomain_entropy


def get_number_of_ips_and_domains_sharing_ip_with(domain, db, domain_mongo_index, ip_mongo_index):
    """
    :param domain: 要查询的域名
    :param domain_mongo_index:
    :param ip_mongo_index:
    :return:
        number_of_domains： 与此域名共享ip的域名个数
        ip_count：此域名映射到的ip地址个数
    """
    number_of_domains, ip_count, subdomain_count = 0, 0, 0
    ip_entropy, subdomain_entropy = 0.0, 0.0
    query_body = {DOMAIN_2ND_FIELD: domain}
    rec = db[domain_mongo_index].find(query_body)
    if rec.count() > 0:
        domain_info = rec[0]
        ips = domain_info[IP_FIELD]
        subdomains = domain_info.get(SUBDOMAINS_FIELD, [])
        ip_entropy = cal_ip_entropy(ips)
        subdomain_entropy = cal_subdomain_entropy(subdomains)
        subdomain_count = len(subdomains)
        ip_count = len(ips)
        if not ip_count:
            print("notexist domain: %s" % domain)
        for ip in ips:
            query_body = {IP_FIELD: ip}
            ans = db[ip_mongo_index].find(query_body)
            if ans.count() > 0:
                domains = ans[0][DOMAIN_2ND_FIELD]
                number_of_domains += len(domains) - 1  # -1是为了减掉自己
    return ip_count, number_of_domains, subdomain_count, ip_entropy, subdomain_entropy


def ip_feature2csv(domain_list, db, domain_mongo_index, ip_mongo_index, ip_feature_file):
    ip_feature_dict_list = []
    iter = 0
    for domain in domain_list:
        ip_count, number_of_domains, subdomain_count, ip_entropy, subdomain_entropy = get_number_of_ips_and_domains_sharing_ip_with(
            domain, db, domain_mongo_index, ip_mongo_index)
        print("domain: %s, ip_count: %s" % (domain, ip_count))
        ip_feature_dict = {
            DOMAIN_2ND_FIELD: domain, NUMBER_OF_UNIQUE_IPS: ip_count, NUMBER_OF_UNIQUE_SUBDOMAINS: subdomain_count,
            NUMBER_OF_DOMAINS: number_of_domains,
            IP_ENTROPY: ip_entropy, SUBDOMAIN_ENTROPY: subdomain_entropy
        }
        ip_feature_dict_list.append(ip_feature_dict)

        print("iter: %s, domain: %s" % (iter, domain))
        iter += 1

    df = pd.DataFrame(ip_feature_dict_list,
                      columns=[DOMAIN_2ND_FIELD, NUMBER_OF_UNIQUE_IPS, NUMBER_OF_UNIQUE_SUBDOMAINS, NUMBER_OF_DOMAINS,
                               IP_ENTROPY, SUBDOMAIN_ENTROPY])
    df.sort_values(by=NUMBER_OF_UNIQUE_IPS)
    df.to_csv(ip_feature_file, index=True)


def ip_sharing_counter(domain_list, ip_feature_file):
    start_time = time.time()
    ip_feature2csv(domain_list, db_bad, domain_mongo_index, ip_mongo_index, ip_feature_file)
    end_time = time.time()
    cost_time = (end_time - start_time) / 60
    print("handler bad domains, cost_time: %s minutes" % (cost_time))


if __name__ == '__main__':
    # domain_bad = 1
    domain_bad = 0
    bad_url_domains = read_domain_file(BAD_URL_DOMAINS_FILE)  # 加载已经从niclog_url日志中查询到的恶意域名
    good_url_domains = read_domain_file(GOOD_URL_DOMAINS_FILE)  # 加载已经从niclog_url日志中查询到的正常域名
    domain_list = bad_url_domains if domain_bad else good_url_domains

    print("len of good_url_domains: %s" % len(good_url_domains))
    print("len of bad_url_domains: %s" % len(bad_url_domains))
    print("len of domains: %s" % len(domain_list))

    ip_feature_file = str(domain_bad) + "_" + IP_FEATURE_FILE
    ip_sharing_counter(domain_list, ip_feature_file)
