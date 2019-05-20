"""
把提取出的不同特征，融合成一个csv文件
"""
import pandas as pd
import numpy as np
import os
from pymongo import MongoClient
from common.mongodb_op import mongo_url
from common.file_names import IP_FEATURE_FILE, AVG_DISTANCE_FILE, CHANGES_DETECTED_FILE, DOMAIN_NAME_FEATURE_FILE, \
    WHOIS_DAYS_GAP_FILE, FINAL_FEATURE_FILE
from common.mongo_common import IP_FIELD, DOMAIN_2ND_FIELD, SUBDOMAINS_FIELD
from common.common_ip_fields import NUMBER_OF_UNIQUE_IPS, NUMBER_OF_UNIQUE_SUBDOMAINS, NUMBER_OF_DOMAINS, IP_ENTROPY, \
    SUBDOMAIN_ENTROPY
from common.common_whois_fields import REGISTER_DAYS, ALIVE_DAYS, UPDATE_DAYS
from common.mongodb_op import FEATURE_MONGO_DB, FEATURE_MONGO_COL
from common.common_time_seq_fields import CHANGE_NUMBERS, CHANGE_MEAN, CHANGE_STD, CHANGE_AMP, AVG_DIS_FIELD
from common.common_domain_name_fields import DOMAIN_LEN, DOMAIN_NAME_ENTROPY, N_DIGITS, DIGIT_NUMBER_RATIO, \
    N_GROUPS_OF_DIGITS, WORD_SEG_GROUP, LONGEST_SUBSTRING_RATIO
from ip_features.load_domains import load_domains

client = MongoClient(mongo_url)
db_feature = client[FEATURE_MONGO_DB]
feature_col = FEATURE_MONGO_COL
DOMAIN_LABEL = "label"


def read_csv_file(csv_file, pri_index, fileds):
    """
    :param csv_file:
    :param pri_index: 是域名在csv文件中的列号
    :param fileds:
    :return:
    """
    df = pd.read_csv(csv_file)
    info_dict = {}
    for i in range(len(df)):
        pri_key = df.loc[i][pri_index]
        d = {field: df.loc[i][field] for field in fileds}
        info_dict[pri_key] = d
    # for item in info_dict.items():
    #     print(item)
    return info_dict


def load_features_dict(domain_bad):
    dir = "data"
    ip_file = os.path.join(dir, str(domain_bad) + "_" + IP_FEATURE_FILE)
    domain_name_file = os.path.join(dir, str(domain_bad) + "_" + DOMAIN_NAME_FEATURE_FILE)
    whois_file = os.path.join(dir, str(domain_bad) + "_" + WHOIS_DAYS_GAP_FILE)
    time_seq_file = os.path.join(dir, str(domain_bad) + "_" + CHANGES_DETECTED_FILE)
    lag = 24
    time_seq_dist_file = os.path.join(dir, str(domain_bad) + "_" + str(lag) + "_" + AVG_DISTANCE_FILE)  # lag=24

    ip_fields = [NUMBER_OF_UNIQUE_IPS, NUMBER_OF_UNIQUE_SUBDOMAINS, NUMBER_OF_DOMAINS,
                 IP_ENTROPY, SUBDOMAIN_ENTROPY]
    whois_fields = [REGISTER_DAYS, ALIVE_DAYS, UPDATE_DAYS]
    time_seq_fields = [CHANGE_NUMBERS, CHANGE_MEAN, CHANGE_STD, CHANGE_AMP]
    time_seq_dist_fields = [AVG_DIS_FIELD]
    domain_name_fields = [DOMAIN_LEN, DOMAIN_NAME_ENTROPY, N_DIGITS, DIGIT_NUMBER_RATIO,
                          N_GROUPS_OF_DIGITS, WORD_SEG_GROUP, LONGEST_SUBSTRING_RATIO]

    fields = [DOMAIN_2ND_FIELD] + ip_fields + time_seq_fields + domain_name_fields + whois_fields

    ip_dict = read_csv_file(ip_file, 1, ip_fields)
    domain_name_dict = read_csv_file(domain_name_file, 1, domain_name_fields)
    whois_dict = read_csv_file(whois_file, 1, whois_fields)
    time_seq_dict = read_csv_file(time_seq_file, 1, time_seq_fields)
    time_seq_dist_dict = read_csv_file(time_seq_dist_file, 1, time_seq_dist_fields)
    return fields, ip_dict, domain_name_dict, whois_dict, time_seq_dict, time_seq_dist_dict


def combine_features(domain_bad):
    fields, ip_dict, domain_name_dict, whois_dict, time_seq_dict, time_seq_dist_dict = load_features_dict(domain_bad)

    # 域名文件所在目录：G:\git-place\es_log_analysis\ip_features
    relative_dir = os.path.join(os.path.dirname(os.getcwd()), "ip_features")
    print("relative_dir: ", relative_dir)
    domains = load_domains(domain_bad, relative_dir)

    domain_info_list = []
    for domain in domains:
        ip_feature = ip_dict.get(domain, {})
        whois_feature = whois_dict.get(domain, {})
        domain_name_feature = domain_name_dict.get(domain, {})
        time_seq_feature = time_seq_dict.get(domain, {})
        time_seq_dist_feature = time_seq_dist_dict.get(domain, {})
        d = dict(list(ip_feature.items()) + list(whois_feature.items()) + list(domain_name_feature.items()) + list(
            time_seq_feature.items()) + list(time_seq_dist_feature.items()))
        d[DOMAIN_2ND_FIELD] = domain
        d[DOMAIN_LABEL] = domain_bad
        domain_info_list.append(d)

    fields = fields + [DOMAIN_LABEL]
    df = pd.DataFrame(domain_info_list, columns=fields)
    file = str(domain_bad) + "_" + FINAL_FEATURE_FILE
    df.to_csv(file, index=True)


if __name__ == '__main__':
    domain_bad = 0
    combine_features(domain_bad)
    file0 = str(domain_bad) + "_" + FINAL_FEATURE_FILE

    domain_bad = 1
    combine_features(domain_bad)
    file1 = str(domain_bad) + "_" + FINAL_FEATURE_FILE
