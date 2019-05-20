"""

"""
import os
import math
import numpy as np
import pandas as pd
from common.date_op import differate_one_day_more, change_date_str_format, days_offset
from common.other_common import remove_file
from common.mongo_common import DOMAIN_2ND_FIELD
from common.draw_picture import draw_scatter, draw_bar, draw_two_bar, draw_line
from time_features.extract_time_seq2csv import csv2df, TIME_SEQ_FILE
from time_features.cusum_detection import detect_cusum, plot_result
from common.url_log_domain_common import START_DAY, DAY_RANGE

HOURS_IN_ONE_DAY = 24
EPROCH = 8
AVG_DISTANCE_FILE = "avg_dis.csv"
CHANGES_DETECTED_FILE = 'changes.csv'
AVG_DIS_FIELD = "avg_dis"
LABEL_FIELD = "label"
ABNORMAL_VISIT = "abnormal_visit"

Z_SCORE_ANORMAL_FIG = "z_score/"
CUSUM_ANORMAL_FIG = "cusum/"
DISTANCE_FILE = 'distance/'
CHANGE_NUMBERS = "change_numbers"
CHANGE_MEAN = "change_duration_mean"
CHANGE_STD = "change_duration_std"
CHANGE_AMP = "change_amp"


def normialized(p_time_seq):
    """将时间序列进行标准化，服从正态分布"""
    try:
        mean = np.mean(p_time_seq)
        std = np.std(p_time_seq)
        if std:
            p_time_seq_norm = (p_time_seq - mean) / std
        else:
            p_time_seq_norm = p_time_seq
        return p_time_seq_norm
    except Exception as e:
        print("normialized error: %s" % (e,))


def min_max_normalize(p_time_seq):
    """将时间序列进行min-max归一化"""
    try:
        min_val = np.min(p_time_seq)
        max_val = np.max(p_time_seq)
        p_time_seq_norm = (p_time_seq - min_val) / (max_val - min_val)
        return p_time_seq_norm
    except Exception as e:
        print("normialized error: %s" % (e,))


def get_time_seq_iter_counter(time_seq):
    """
    当时间序列长度为8时，应该只循环一次，即iter_counter=1，因此这里应该是时间序列长度 - 7
    :param time_seq:
    :return:
    """
    iter_counter = 1
    iter_counter = iter_counter if time_seq.size <= 8 else time_seq.size - 7
    # print("292929 iter_counter: %s" % (iter_counter,))
    return iter_counter


def one_hour2one_period(time_seq, eproah=EPROCH):
    """
        构建滑动窗口
        将Pt转化为Pt-和Pt+
        Pt-=以t时刻为中心，往前eproch个时刻的查询次数之和,
        Pt+=以t时刻为中心，往后eproch个时刻的查询次数之和,
    :param time_seq: 单个域名每一个的时间序列或者在一个实验期（可能是7天或者15天等）内的访问次数序列
    :param eproah:8， 将每8个查询次数作为一组，利用滑动窗口在整个时间序列上移动，需要移动iter_counter次
        当时间序列长度为168时，iter_counter=161，即需要从i=0移动到i=160
    :return:
    """
    iter_counter = get_time_seq_iter_counter(time_seq)
    ans = np.array([])
    for i in range(iter_counter):
        this_sum = 0
        for num in time_seq[i:i + eproah]:
            this_sum += num
        this_sum /= eproah
        ans = np.append(ans, this_sum)
    return ans


def cal_euclidean_distance(time_seq1, time_seq2):
    """
    计算两个时间序列之间的欧式距离
    :param time_seq1:
    :param time_seq2:
    :return:
    """
    return np.sum(np.square(time_seq1 - time_seq2))


def cal_domains_distance(one_dom_time_seq_dict):
    """
    通过计算每一天的时间序列之间的欧式距离来验证单个域名在每一天是否有重复的模式
    :return:
    """
    total_dis = 0
    for d1 in one_dom_time_seq_dict:
        for d2 in one_dom_time_seq_dict:
            if d1 == d2:
                continue
            else:
                time_seq1 = one_dom_time_seq_dict[d1]
                time_seq2 = one_dom_time_seq_dict[d2]
                total_dis += cal_euclidean_distance(time_seq1, time_seq2)
    n = len(one_dom_time_seq_dict)
    div = (n - 1) * (n - 2) // 2
    # div = (n - 1) * n // 2
    print("total_dis: %s, n: %s, div: %s" % (total_dis, n, div))
    avg_dis = total_dis / div
    return avg_dis


def cal_domains_distance_v1(sorted_time_seq, period_length):
    """
    通过计算每一天的时间序列之间的欧式距离来验证单个域名是否存在重复的模式
    :param sorted_time_seq:
    :return:
    """
    # sorted_time_seq = min_max_normalize(sorted_time_seq)  #min-max 归一化
    sorted_time_seq = normialized(sorted_time_seq)  # 标准化
    total_dis = 0
    n = len(sorted_time_seq)
    # print("n= %s" % n)
    i = 0
    while i < n:
        seq1 = sorted_time_seq[i:i + period_length]
        j = i + period_length
        while j < n:
            seq2 = sorted_time_seq[j:j + period_length]
            total_dis += cal_euclidean_distance(seq1, seq2)
            # print("i: %s, j: %s," % (i, j))
            # print("seq1: %s, seq2: %s," % (seq1, seq2))
            j += period_length
        i += period_length
    # div = (n - 1) * (n - 2) // 2
    div = (n - 1) * n // 2
    avg_dis = total_dis / div
    return avg_dis


def multi_days_time_seq(time_seq_dict, period_length=7):
    """
    :param time_seq_dict:
    :param period_length: 考察的时间，单位为天数， 日期之间未必连续,因为在不连续的日期之间补上24 * 缺少的天数个0
    如时间序列中的日期为
    :return:
    """
    sorted_time_seq_tuple = sorted(time_seq_dict.items(), reverse=False)
    sorted_time_seq = np.array([])

    # print("=======================stary===============================")
    # print("len of sorted_time_seq_tuple: %s" % (len(sorted_time_seq_tuple)))
    # 取出有序时间序列元组中的第一个日期，若它在记录起始日期之后，则计算它与起始日期的距离，并加上相应个数的0
    ret_start_date = str(sorted_time_seq_tuple[0][0])  # 这里的sorted_time_seq_tuple[0][0]为什么会是Numpy.int64类型呢
    seq_start_date = days_offset(change_date_str_format(START_DAY), -1 * (DAY_RANGE - 1))
    days_gap = differate_one_day_more(seq_start_date, ret_start_date)
    if days_gap >= 0:  # 说明有序时间序列中的起始日期与记录起始日期不同
        tmp_time_seq = np.zeros(HOURS_IN_ONE_DAY * (days_gap + 1))
        sorted_time_seq = np.append(sorted_time_seq, tmp_time_seq)
        # print("add %s days zero" % days_gap)
    # print("ret_start_date: %s, seq_start_date: %s, days_gap: %s" % (ret_start_date, seq_start_date, days_gap))

    for index in range(len(sorted_time_seq_tuple)):
        date_str = str(sorted_time_seq_tuple[index][0])
        time_seq = sorted_time_seq_tuple[index][1]
        if index:
            day_before = str(sorted_time_seq_tuple[index - 1][0])
            days_gap = differate_one_day_more(day_before, date_str)
            # print("day_before: ", day_before, "date_str: ", date_str, "days_gap: ", days_gap)
            # 两个相邻的时间序列之间相差不止一天， 那么缺少的这些天都是24个0，即这两个日期之间的日期中的查询次数都是0
            if days_gap >= 0:
                tmp_time_seq = np.zeros(HOURS_IN_ONE_DAY * days_gap)
                sorted_time_seq = np.append(sorted_time_seq, tmp_time_seq)
        sorted_time_seq = np.append(sorted_time_seq, time_seq)
        # print("date_str: %s, len of sorted_time_seq: %s" % (date_str, len(sorted_time_seq)))

    # 取出有序时间序列元组中的最后一个日期，若它在记录截止日期之前，则计算它与截止日期的距离，并加上相应个数的0
    ret_end_date = str(sorted_time_seq_tuple[-1][0])  # 这里的sorted_time_seq_tuple[0][0]为什么会是Numpy.int64类型呢
    seq_end_date = change_date_str_format(START_DAY)
    days_gap = differate_one_day_more(ret_end_date, seq_end_date)
    if days_gap >= 0:  # 说明有序时间序列中的截止日期与记录截止日期不同
        tmp_time_seq = np.zeros(HOURS_IN_ONE_DAY * (days_gap + 1))
        sorted_time_seq = np.append(sorted_time_seq, tmp_time_seq)
    # print("ret_end_date: %s, seq_end_date: %s, days_gap: %s" % (ret_end_date, seq_end_date, days_gap))

    # print("192 len of sorted_time_seq: %s" % (len(sorted_time_seq)))
    return sorted_time_seq


def get_change_duration_std(start_spots, ending_spots):
    """
    :param start_spots: 突变点起点序列
    :param ending_spots: 突变点终点序列
    :return: 突变点duration均值与方差
    """
    durations = []
    for start_point, end_point in zip(start_spots, ending_spots):
        durations.append(end_point - start_point + 1)
    durations = np.array(durations)
    return durations.mean(), durations.std()


def show_gp_etc(alarm_spots, start_spots, ending_spots, amp, gp, gn):
    print("======================gp====================")
    print(gp > 0)
    print("=======================gn====================")
    print(gn > 0)
    print("=======================alarm_spots====================")
    print(alarm_spots)
    print("=======================amp====================")
    print(amp)


def analysize_one_domain_time_seq(sorted_time_seq, file, show=False):
    """
    使用cusum算法检测异常点
    :param sorted_time_seq:
    :param title:
    :return:
        突变点个数，（突变发生的）起点序列，（突变发生的）终点序列，累积变化幅度(在各次突变过程中突变的总幅度)序列
    """
    p_time_seq = one_hour2one_period(sorted_time_seq)  # p_time_seq为划分了时间窗口后的平均访问序列
    p_time_seq_norm = normialized(p_time_seq)

    # print("============v=====p_time_seq_norm==================")
    # print(p_time_seq_norm)
    threshold, drift = 0.4, 0.1
    ending = True
    alarm_spots, start_spots, ending_spots, amp, gp, gn = detect_cusum(p_time_seq_norm, threshold, drift, ending)

    # show_gp_etc(alarm_spots, start_spots, ending_spots, amp, gp, gn)
    if show:
        plot_result(p_time_seq_norm, threshold, drift, ending, alarm_spots, start_spots, ending_spots, gp, gn, file)
    change_numbers = alarm_spots.size
    return change_numbers, start_spots, alarm_spots, ending_spots, amp


def get_sorted_time_seq_of_domains(time_seq_nesting_dict):
    """
    获得各个域名的有序时间序列
    :param time_seq_nesting_dict:各个域名的有序时间序列组成的字典，键为域名，值为对应的时间序列
    :return:
    """
    sorted_time_seq_dict = {}
    for domain in time_seq_nesting_dict:
        #  按照时间顺序将访问次数序列拼接起来，其中每24个访问计数为一天
        # print("domain: %s multi_days_time_seq" % (domain))
        sorted_time_seq = multi_days_time_seq(time_seq_nesting_dict[domain])
        if sorted_time_seq.size < DAY_RANGE * HOURS_IN_ONE_DAY:
            # print("domain: %s is derived: %s" % (domain, sorted_time_seq.size))
            continue
        elif sorted_time_seq.size > DAY_RANGE * HOURS_IN_ONE_DAY:
            sorted_time_seq = sorted_time_seq[:DAY_RANGE * HOURS_IN_ONE_DAY]
        sorted_time_seq_dict[domain] = sorted_time_seq
    # print("len of sorted_time_seq_dict: %s" % (len(sorted_time_seq_dict)))
    return sorted_time_seq_dict


def change_info2csv(domain_list, numbres_list, mean_list, std_list, amp_list, domain_bad):
    """将突变点个数、duration_mean、duration_std等信息存入文件中"""
    d = {
        DOMAIN_2ND_FIELD: domain_list, CHANGE_NUMBERS: numbres_list, CHANGE_MEAN: mean_list,
        CHANGE_STD: std_list, CHANGE_AMP: amp_list, LABEL_FIELD: domain_bad
    }
    df = pd.DataFrame(data=d)
    df.sort_values(by=CHANGE_NUMBERS)
    csv_file = str(domain_bad) + "_" + CHANGES_DETECTED_FILE
    remove_file(csv_file)
    df.to_csv(csv_file, index=True)


def detect_abnormal_points_with_cusum(sorted_time_seq_dict, domain_bad, number2csv=False):
    numbres_list, mean_list, std_list, amp_list = [], [], [], []
    domain_list = []
    # print("detect_abnormal_points_with_cusum len of domains: %s" % (len(sorted_time_seq_dict.items())))
    for domain, sorted_time_seq in sorted_time_seq_dict.items():
        domain_list.append(domain)
        file = CUSUM_ANORMAL_FIG + str(domain_bad) + "_" + domain + ".png"
        remove_file(file)
        # change_numbers:突变点个数
        # start_spots: 各次突变发生的起点组成的序列
        # ending_spots: 各次突变发生的终点组成的序列
        # alarm_spots：各次突变被检测到的时间点组成的序列
        # change_numbers, start_spots, alarm_spots, ending_spots, amp = analysize_one_domain_time_seq(sorted_time_seq,
        #                                                                                             file, True)
        change_numbers, start_spots, alarm_spots, ending_spots, amp = analysize_one_domain_time_seq(sorted_time_seq,
                                                                                                    file, False)
        # print("domain: %s, starting_spots: %s" % (domain, start_spots))
        # print("amp: ", len(amp))
        # duration_mean，duration_std：域名每次突变的时间间隔的均值和方差
        if len(start_spots):
            duration_mean, duration_std = get_change_duration_std(start_spots, ending_spots)
        else:
            duration_mean, duration_std = 0.0, 0.0
        numbres_list.append(change_numbers)
        mean_list.append(duration_mean)
        std_list.append(duration_std)
        amp_list.append(np.array(amp).mean() if len(amp) else 0)  # 域名各次突变的幅度的均值

    if number2csv:
        change_info2csv(domain_list, numbres_list, mean_list, std_list, amp_list, domain_bad)


def detect_abnormal_points_with_cusum_dup(sorted_time_seq_dict, domain_bad):
    numbres_list, mean_list, std_list = [], [], []
    abnormal_visit_list = []
    domain_list = []
    for domain, sorted_time_seq in sorted_time_seq_dict.items():
        domain_list.append(domain)
        file = CUSUM_ANORMAL_FIG + str(domain_bad) + "_" + domain + ".png"
        remove_file(file)
        change_numbers, start_spots, alarm_spots, ending_spots, amp = analysize_one_domain_time_seq(sorted_time_seq,
                                                                                                    file)
        # print("domain: %s, starting_spots: %s" % (domain, start_spots))

        duration_mean, duration_std = get_change_duration_std(start_spots, ending_spots)
        # print("type of change_numbers: %s" % (type(change_numbers)))
        numbres_list.append(change_numbers)
        mean_list.append(duration_mean)
        std_list.append(duration_std)

    # 将突变点个数、duration_mean、duration_std等信息存入文件中
    d = {
        DOMAIN_2ND_FIELD: domain_list, CHANGE_NUMBERS: numbres_list, CHANGE_MEAN: mean_list,
        CHANGE_STD: std_list, LABEL_FIELD: domain_bad
        # , ABNORMAL_VISIT: abnormal_visit_list
    }
    df = pd.DataFrame(data=d)
    df.sort_values(by=CHANGE_NUMBERS)
    csv_file = str(domain_bad) + "_" + CHANGES_DETECTED_FILE
    remove_file(csv_file)
    df.to_csv(csv_file, index=True)


def read_changes_csv_file(domain_bad):
    """
    读取域名的突变点个数、突变点发生时的duration
    :param domain_bad:
    :return:
    """
    csv_file = str(domain_bad) + "_" + CHANGES_DETECTED_FILE
    df = pd.read_csv(csv_file)
    x, y, z, labels = np.array([]), np.array([]), np.array([]), np.array([])
    for i in range(len(df)):
        mean = df.loc[i][CHANGE_MEAN]
        std = df.loc[i][CHANGE_STD]
        numbers = df.loc[i][CHANGE_NUMBERS]
        x = np.append(x, numbers)
        y = np.append(y, std)
        z = np.append(z, mean)
        labels = np.append(labels, domain_bad)
    return x, y, z, labels


def softmax_number(x):
    exp_x = np.exp(x)
    softmax_x = exp_x / np.sum(exp_x)
    return softmax_x


def cal_distance_within_time_window(sorted_time_seq_dict, domain_bad):
    """
    计算各域名在一段时间窗口内的模式相似度
    :param time_seq_dict: 各域名及其有序时间序列组成的字典
    :param domain_bad:
    :return:
    """
    # lag = int(input("please enter a different value for lag, for default lag is %s" % HOURS_IN_ONE_DAY))
    lag = HOURS_IN_ONE_DAY
    # lag = 8
    visited_domains, avg_distances = [], []
    total_counter = 0

    # 不同域名的时间序列所处的时间段不同， 起始日期均不同
    # 对于某一个域名，计算每一个时间段（默认是以一天作为一个时间段）之间的相似度
    for domain in sorted_time_seq_dict:
        print('===================cal_distance_within_time_window====================')
        print("len of sorted_time_seq_dict[%s]: %s" % (domain, len(sorted_time_seq_dict[domain])))
        sorted_time_seq = sorted_time_seq_dict[domain]
        avg_dis = cal_domains_distance_v1(sorted_time_seq, lag)
        visited_domains.append(domain)
        avg_distances.append(avg_dis)
        total_counter += 1

    # 将每个域名每天的相似度写入csv文件中
    labels = np.ones(len(visited_domains), dtype=np.int64) if domain_bad else np.zeros(len(visited_domains),
                                                                                       dtype=np.int64)
    d = {DOMAIN_2ND_FIELD: visited_domains, AVG_DIS_FIELD: avg_distances, LABEL_FIELD: labels}
    df = pd.DataFrame(data=d)
    csv_file = DISTANCE_FILE + str(domain_bad) + "_" + str(lag) + "_" + AVG_DISTANCE_FILE
    remove_file(csv_file)
    df.to_csv(csv_file)


def show_change_number_domain(x, y):
    """
    绘制域名的突变点个数及给定突变点个数时的域名分布情况
    :param x:
    :param y:
    :return:
    """
    print("=================draw_numbers_list====================")
    title = "突变点个数与这样的域名个数的关系"
    xlabel = u"突变点个数"
    ylabel = u"突变点个数为x对应的域名个数"
    color = 'g'
    if domain_bad:  # 恶意域名为红色
        color = 'r'
    draw_bar(x, y, min(x), max(x), title, xlabel, ylabel, color)


def get_real_time_from_window_time(win_no):
    """
    如某个访问发生在实验期的第1内的第9个小时，所以它是在第二个时间窗口内发生了突变
    滑动窗口的编号其实就是访问相对于选取的开始时间（这里是2019.03.28）的偏移
    滑动窗口的起点和滑动窗口的编号一致，滑动窗口的终点是滑动窗口的起点加上7
    :param win_no: 时间序列中的索引，滑动窗口的编号
    :return: win_no指定的滑动窗口的开始时间和结束时间
    """
    day_no = win_no / HOURS_IN_ONE_DAY
    start_time = win_no - day_no * HOURS_IN_ONE_DAY
    day_no = (win_no + EPROCH - 1) / HOURS_IN_ONE_DAY
    end_time = (win_no + EPROCH) - day_no * HOURS_IN_ONE_DAY
    return day_no, start_time, end_time


def check_abnormal_visit_v1(alarm_spots):
    """
    发生在凌晨的异常访问的数量
    """
    abnormal_count = 0
    for alarm_spot in alarm_spots:
        day_no, start_time, _ = get_real_time_from_window_time(alarm_spot)

        def set_condition(time_num):
            return time_num >= 2 and time_num <= 8

        if set_condition(start_time):
            abnormal_count += 1
    return abnormal_count


def global_analysize(time_seq_nesting_dict, domain_bad):
    """
    :param time_seq_nesting_dict: 各域名的时间序列， 如{domain: { "20190321" : []}}
    获取各域名的有序时间序列，并计算各域名时间序列在给定时间窗口内的相似度，时间窗口大小选取8和24小时，
    寻找异常点，并计算
    """
    sorted_time_seq_dict = get_sorted_time_seq_of_domains(time_seq_nesting_dict)
    cal_distance_within_time_window(sorted_time_seq_dict, domain_bad)  # sorted_time_seq_dict为原始访问次数序列
    detect_abnormal_points_with_cusum(sorted_time_seq_dict, domain_bad, True)


if __name__ == '__main__':
    # domain_bad = int(input("please enter what kind of domains to get: 0 for good doamins, 1 for bad domains"))
    # domain_bad = 1
    domain_bad = 0
    time_seq_file = str(domain_bad) + "_" + TIME_SEQ_FILE
    time_seq_nesting_dict = csv2df(time_seq_file)
    print("len of time_seq_nesting_dict: %s" % (len(time_seq_nesting_dict)))
    global_analysize(time_seq_nesting_dict, domain_bad)
