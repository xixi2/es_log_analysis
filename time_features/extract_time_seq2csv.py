"""
生成时间序列文件
分析提取出的访问频率，检测异常点
"""
import pandas as pd
from pymongo import MongoClient
from common.date_op import days_offset, differate_one_day_more, change_date_str_format
from common.mongodb_op import mongo_url
from common.other_common import remove_file
from common.mongodb_op import NIC_LOG_MONGO_DB, BAD_DOMAINS_COUNTER2ND_MONGO_INDEX, GOOD_DOMAINS_COUNTER2ND_MONGO_INDEX
from common.mongo_common import DOMAIN_2ND_FIELD, SUBDOMAINS_FIELD, IP_FIELD, DATE_FIELD
from common.domains_op import read_domain_file
from common.url_log_domain_common import GOOD_URL_DOMAINS_FILE, BAD_URL_DOMAINS_FILE
from common.common_niclog_url import DOMAIN_URL_VISITING, GOOD_DOMAIN_URL_VISITING, DOMAIN_FREQUENCY_COUNTING_COL
from common.url_log_domain_common import START_DAY, DAY_RANGE

TIME_SEQ_FIELD = "time_seq"
TIME_SEQ_FILE = TIME_SEQ_FIELD + ".csv"
client = MongoClient(mongo_url)
db_basic = client[DOMAIN_URL_VISITING]
db_good_basic = client[GOOD_DOMAIN_URL_VISITING]
col_frequency = DOMAIN_FREQUENCY_COUNTING_COL  # 存储域名访问次数的MongoDB集合
db_dict = {0: db_good_basic, 1: db_basic}  # 存储域名访问次数的MongoDB数据库


def csv2df(time_seq_file):
    """
    读取时间序列文件TIME_SEQ_FILE，返回一个dict，dict的键值是域名，每个值还是一个dict
    :return:
    """
    df = pd.read_csv(time_seq_file)
    time_seq_dict = {}
    for i in range(len(df)):
        # print("%s, %s" % (i, df.loc[i].values[1:]))
        domain_2nd = df.loc[i].values[1]
        date_str = df.loc[i].values[2]
        one_day_time_seq = df.loc[i].values[3:]  # one_day_time_seq 是ndarray
        # print(domain_2nd, len(one_day_time_seq), type(one_day_time_seq))
        if not time_seq_dict.get(domain_2nd):
            time_seq_dict[domain_2nd] = {}
        time_seq_dict[domain_2nd][date_str] = one_day_time_seq
    print("len(time_seq_dict): %s" % (len(time_seq_dict)))
    return time_seq_dict


def date_older_than_start_date(date_str):
    seq_start_date = days_offset(change_date_str_format(START_DAY), -1 * DAY_RANGE)
    if differate_one_day_more(seq_start_date, date_str) >= 0:
        return False
    return True


def date_younger_than_start_date(date_str):
    seq_end_date = change_date_str_format(START_DAY)
    if differate_one_day_more(seq_end_date, date_str) >= 0:
        return True
    return False


def get_visiting_frequency(domain_bad):
    """
    从MongoDB数据库中读出域名访问频率，形成时间序列，写入到csv文件
    :return:
    """
    # 从MongoDB集合中查询到域名的访问次数，形成csv文件
    db = db_dict[domain_bad]
    recs = db[col_frequency].find()

    print("get_visiting_frequency for %s domains" % (recs.count()))

    vis_dict_list = []
    for rec in recs:
        domain = rec[DOMAIN_2ND_FIELD]
        date_str = rec[DATE_FIELD]
        # if date_older_than_start_date(date_str) or date_younger_than_start_date(date_str):
        #     print("date_str: %s" % (date_str))
        #     continue
        vis_dict = {
            DOMAIN_2ND_FIELD: domain,
            DATE_FIELD: date_str
        }
        for index in range(24):
            index_counter = rec.get(str(index), 0)
            vis_dict[index] = index_counter
        vis_dict_list.append(vis_dict)
    columns_fields = [DOMAIN_2ND_FIELD, DATE_FIELD]
    for index in range(24):
        columns_fields.append(index)
    df = pd.DataFrame(vis_dict_list, columns=columns_fields)
    df.sort_values(by=DOMAIN_2ND_FIELD).sort_values(by=DATE_FIELD)
    time_seq_file = str(domain_bad) + "_" + TIME_SEQ_FILE
    remove_file(time_seq_file)
    df.to_csv(time_seq_file, index=True)


if __name__ == '__main__':
    # domain_bad = 1
    domain_bad = 0
    get_visiting_frequency(domain_bad)
    time_seq_file = str(domain_bad) + "_" + TIME_SEQ_FILE
    # csv2df(time_seq_file)
