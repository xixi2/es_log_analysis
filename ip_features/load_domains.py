import os
from common.domains_op import read_domain_file
from common.url_log_domain_common import GOOD_URL_DOMAINS_FILE, BAD_URL_DOMAINS_FILE


def load_domains(domain_bad, relative_dir):
    bad_file = os.path.join(relative_dir, BAD_URL_DOMAINS_FILE)
    good_file = os.path.join(relative_dir, GOOD_URL_DOMAINS_FILE)
    print("bad_file: ", bad_file)
    bad_domains = read_domain_file(bad_file)  # 加载已经从niclog_url日志中查询到的恶意域名
    good_domains = read_domain_file(good_file)  # 加载已经从niclog_url日志中查询到的正常域名
    domain_list = bad_domains if domain_bad else good_domains
    return domain_list


if __name__ == '__main__':
    load_domains(0, "")
