from pymongo import MongoClient
from common.mongo_common import DOMAIN_2ND_FIELD

Host = '192.168.105.140'
Port = 27017
User = 'mongo123'
Password = 'mongo123'
database = 'test'

mongo_url = 'mongodb://%s:%s@%s:%s' % (User, Password, Host, Port)

# 恶意域名
MAL_DOMS_MONGO_DB = "malicious_domains"
MAL_DOMS_MONGO_INDEX = "mal_domains"  # 修改前是这集合，现在是下面这个集合
# MAL_DOMS_MONGO_INDEX = "vicious_domains"  # 这个域名集合相较上面那个，修改每个域名的来源和恶意类型
MAL_DOMAINS_MONGO_INDEX = "vicious_domains"

# 主动结点
ACTIVE_MONGO_DB = "active_domain_ip_resolutions"
ACTIVE_DOM_TO_IP_MONGO_INDEX = "active_domain2ip"
ACTIVE_DOM_TTL_TO_MONGO_INDEX = "active_domain2ip_ttl"
ACTIVE_DOM_NAMESERVER_MONGO_INDEX = "active_domain2nameserver"
ACTIVE_DOM_NAMERSERVER_TTL_MONGO_INDEX = "active_domain2namerserver_ttl"
ACTIVE_NAMESERVER_TO_IP_MONGO_INDEX = "active_nameserver2ip"
ACTIVE_NAMERSER_TO_IP_TTL_MONGO_INDEX = "active_nameserver2ip_ttl"

# niclog访问记录
NIC_LOG_MONGO_DB = "nic_log_visiting"
NIC_LOG_BAD_FULL_NAME_VISITING_MONGO_INDEX = "bad_full_domains_visiting_records"
NIC_LOG_BAD_DOMAIN_SUBDOMAINS_MONGO_INDEX = "bad_domain_subdomain"
NIC_LOG_GOOD_DOMAIN_SUBDOMAINS_MONGO_INDEX = "good_domain_subdomain"
NIC_LOG_GOOD_FULL_NAME_VISITING_MONGO_INDEX = "good_full_domains_visiting_records"

# 域名访问次数，用以形成时间序列的
BAD_DOMAINS_COUNTER2ND_MONGO_INDEX = "bad_domains_counter_2nd"  # 统计niclog恶意域名的访问次数，以提取时间特征==》二级域名
BAD_DOMAINS_COUNTER3TH_MONGO_INDEX = "bad_domains_counter_3th"  # 统计niclog恶意域名的访问次数，以提取时间特征==》三级域名
GOOD_DOMAINS_COUNTER2ND_MONGO_INDEX = "good_domains_counter_2nd"  # 统计niclog恶意域名的访问次数，以提取时间特征==》二级域名
GOOD_DOMAINS_COUNTER3TH_MONGO_INDEX = "good_domains_counter_3th"  # 统计niclog恶意域名的访问次数，以提取时间特征==》三级域名

# 正常域名
GOOD_DOMAINS_MONGO_DB = "good_domains"
GOOD_DOMAINS_MONGO_INDEX = "good_domains"

# 正常域名和恶意域名均有这些集合,
DOMAIN_IP_RESOLUTION_MONGO_INDEX = "domain_ips"
DOMAIN_SUBDOMAIN_MONGO_INDEX = "domain_subdomains"
DOMAIN_WHOIS_MONGO_INDEX = "domain_whois"

# 域名和IP解析结果
DOMAIN_IP_RESOLUTION_MONGO_DB = "domain_ip_resolution"
BAD_DOMAIN_IP_MONGO_INDEX = "bad_domain2ip"
GOOD_DOMAIN_IP_MONGO_INDEX = "good_domain2ip"
GOOD_IPS_MONGO_INDEX = "good_ips"  # 正常域名解析得到的ip
BAD_IPS_MONGO_INDEX = "bad_ips"  # 恶意域名解析得到的ip

# 为了找到更多的恶意域名，采取从niclog中匹配域名，然后使用virustotal进行验证的方法
UNCERTAIN_NICLOG_MONGO_DB = "uncertain_niclog_domains"
UNCERTAIN_NICLOG_MONGO_INDEX = "uncertain_niclog_domains"

# 形成最终的csv文件的数据库
FEATURE_MONGO_DB = "features"
FEATURE_MONGO_COL = "feature_col"


def query_mongodb_by_body(client, db_name, mongo_index, fields=None, query_body=None):
    recs_list = []
    db = client[db_name]
    if query_body:
        recs = db[mongo_index].find(query_body)
    else:
        recs = db[mongo_index].find()
    # print("files: %s" % fields)

    for item in recs:
        temp = []
        if fields:
            if len(fields) > 1:
                for field in fields:
                    temp.append(item[field])
                recs_list.append(tuple(temp))
            else:
                recs_list.append(item[fields[0]])
        else:
            temp = [val for key, val in item.items()]
            recs_list.append(tuple(temp))
    return recs_list


def load_bad_or_good_domains(client, domain_bad):
    """
    返回恶意域名数据集中所有的恶意域名或者正常域名
    :param domain_bad:
    :return:
    """
    fields = [DOMAIN_2ND_FIELD, ]
    if domain_bad:
        # 取出mongodb中所有的恶意域名
        domain_list = query_mongodb_by_body(client, MAL_DOMS_MONGO_DB, MAL_DOMS_MONGO_INDEX, fields)
    else:  # 从mongodb中取出所有的正常域名
        domain_list = query_mongodb_by_body(client, GOOD_DOMAINS_MONGO_DB, GOOD_DOMAINS_MONGO_INDEX, fields)
    return domain_list


def save_domain_subdomains2mongodb(domain, subdomains, db, mongo_index):
    """
    :param domain: 将从Niclog中匹配到的恶意域名记录到mongodb数据库中
    :param subdomains:
    :return:
    """
    # 这里使用addset有问题，addset只能将一个元素加入到已有数组中，无法将多个元素加入到原始数组中
    query_body = {"domain": domain}
    basic_body = {"$addToSet": {"subdomains": {"$each": subdomains}}}
    db[mongo_index].update(query_body, basic_body, True)


def get_all_domains(client, domain_bad):
    """
    从域名数据集中取出所有需要查询的域名
    :param domain_bad:
    :return:
    """
    if domain_bad:  # 取出mongodb中所有的恶意域名
        domain_list = query_mongodb_by_body(client, MAL_DOMS_MONGO_DB, MAL_DOMS_MONGO_INDEX, [DOMAIN_2ND_FIELD])
    else:  # 从mongodb中取出所有的正常域名
        domain_list = query_mongodb_by_body(client, GOOD_DOMAINS_MONGO_DB, GOOD_DOMAINS_MONGO_INDEX, [DOMAIN_2ND_FIELD])
    return domain_list
