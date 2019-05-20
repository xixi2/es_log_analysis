"""
这个文件是为了从alexa_top1m正常域名符串中提取特征，具体特征提取方法见：恶意域名字符串(pdf文件)
"""
import os
import math
import csv
import pandas as pd
from common.mongo_common import DOMAIN_2ND_FIELD
from common.domains_op import keep_2nd_dom_name
from common.other_common import remove_file
from ip_features.load_domains import load_domains
from common.domains_op import write2file
from domain_name.domain_name_word_segment import word_segment, get_longest_meaningful_substring_v0
from common.file_names import DOMAIN_NAME_FEATURE_FILE, LONGEST_SUBSTRING_FILE

good_domain_table = "good_domains"

DOMAIN_LEN = "domain_len"
DOMAIN_NAME_ENTROPY = "domain_name_entropy"
N_DIGITS = "digit_number"
DIGIT_NUMBER_RATIO = "digit_number_ratio"
N_GROUPS_OF_DIGITS = "digit_group"  # 整个二级域名字符串可以被多少组数字分隔开
WORD_SEG_GROUP = "word_group"  # 整个二级域名中字符串最为被分为了多少组如w3cschool最后被分为三组：w, c,school
LONGEST_SUBSTRING_LEN = "longest_sub_len"  # 最长有意义字符串长度，最长有意义子串
LONGEST_SUBSTRING_RATIO = "longest_substring_ratio"


def extract2level_domain(good_domain):
    name_list = good_domain.split('.')
    # print('name_list: {0}'.format(name_list[-2]))
    return name_list[-2]


def write2csv(info_dict_list, columns_fields, file, sorted_field=None):
    """
    将info_dict_list写入csv文件中
    """
    df = pd.DataFrame(info_dict_list, columns=columns_fields)
    if sorted_field:
        df.sort_values(by=sorted_field).sort_values(by=sorted_field)
    remove_file(file)
    df.to_csv(file, index=True)


def cal_domain_name_entropy(domain_name):
    """
    统计域名中每个字符串出现的评率p(c)，最后计算-zigma(p(c)*log(pc(c)) c = 0,1,1,..,len(domain_name)-1
    :return:
    """
    freq = {}
    for ch in domain_name:
        if ch not in freq:
            freq[ch] = 0
        freq[ch] += 1
    sum = 0.0
    for ch in freq:
        p_ch = freq[ch] / len(domain_name)
        sum -= freq[ch] * math.log(p_ch, 2)
    return sum


def check_domains(domains, domain_bad, batch_num=50):
    i = 0
    domain_info_dict_list = []
    longest_substring_list = set()
    for domain in domains:
        i += 1
        domain_2nd = keep_2nd_dom_name(domain)
        domain_len = len(domain_2nd)
        n_digits, digit_segs, word_segs = word_segment(domain_2nd)
        digit_number_ratio = n_digits / len(domain)
        n_groups_of_digits = len(digit_segs)  # 整个二级域名字符串可以被多少组数字分隔开
        n_group_of_word_segs = len(word_segs)  # 整个二级域名中字符串最为被分为了多少组如w3cschool最后被分为三组：w, c,school
        longest_len, longest_substring = get_longest_meaningful_substring_v0(word_segs)  # 最长有意义字符串长度，最长有意义子串
        domain_name_entropy = cal_domain_name_entropy(domain)
        longest_substring_list.add(longest_substring)

        print('==============================================================')
        print('domain: {0}, domain_2nd: {1}, digit_segs: {2}, word_segs:{3}'
              .format(domain, domain_2nd, digit_segs, word_segs))
        print('domain_2nd: {0}, n_digits: {1}, n_groups_digits: {2}, n_group_word_segs: {3}'
              .format(domain_2nd, n_digits, n_groups_of_digits, n_group_of_word_segs))
        print('domain_2nd: {0}, longest_len:{1},longest_substring: {2}'
              .format(domain_2nd, longest_len, longest_substring))

        domain_info = {
            DOMAIN_2ND_FIELD: domain, DOMAIN_LEN: domain_len, DOMAIN_NAME_ENTROPY: domain_name_entropy,
            N_DIGITS: n_digits, DIGIT_NUMBER_RATIO: digit_number_ratio, N_GROUPS_OF_DIGITS: n_groups_of_digits,
            WORD_SEG_GROUP: n_group_of_word_segs,
            LONGEST_SUBSTRING_RATIO: longest_len / domain_len  # 最长有意义子串占整个字符串的比例
        }
        domain_info_dict_list.append(domain_info)
        if i % batch_num == 0 or i == len(domains):
            print('第{0}个域名正在统计'.format(i))
            print("==========domain_info==============")

    columns_fields = [DOMAIN_2ND_FIELD, DOMAIN_LEN, DOMAIN_NAME_ENTROPY, N_DIGITS, DIGIT_NUMBER_RATIO,
                      N_GROUPS_OF_DIGITS, WORD_SEG_GROUP, LONGEST_SUBSTRING_RATIO]
    domain_name_file = str(domain_bad) + "_" + DOMAIN_NAME_FEATURE_FILE
    write2csv(domain_info_dict_list, columns_fields, domain_name_file, DOMAIN_2ND_FIELD)

    longest_substring_file = str(domain_bad) + "_" + LONGEST_SUBSTRING_FILE
    remove_file(longest_substring_file)
    write2file(longest_substring_file, longest_substring_list)


if __name__ == '__main__':
    # domains = []
    # domain_bad = 1
    domain_bad = 0
    # print("os.getcwd(): ", os.getcwd())
    # print("os.path.dirname(): ", os.path.dirname(os.getcwd()))
    relative_dir = os.path.join(os.path.dirname(os.getcwd()),
                                "ip_features")  # 域名文件所在目录：G:\git-place\es_log_analysis\ip_features
    print("relative_dir: ", relative_dir)

    domains = load_domains(domain_bad, relative_dir)
    check_domains(domains, domain_bad)
