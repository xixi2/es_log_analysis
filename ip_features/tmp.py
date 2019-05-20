from common.domains_op import keep_2nd_dom_name, keep_only_3th_dom_name, is_domain_ip, read_domain_file, write2file
from common.url_log_domain_common import GOOD_URL_DOMAINS_FILE

if __name__ == '__main__':
    already_domains = read_domain_file("good_niclog_url_0.txt")
    new_domains = read_domain_file(GOOD_URL_DOMAINS_FILE)
    no_domains = new_domains - already_domains
    print("new_domains: %s" % len(new_domains))
    print("new_domains: %s" % new_domains)
