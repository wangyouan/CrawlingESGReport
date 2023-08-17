#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: download_report_from_cninfo
# @Date: 2023/8/18
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os
import time
import calendar
import datetime
import requests

from tqdm.notebook import tqdm
import pandas as pd
from pandas import DataFrame

QUERY_URL = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/79.0.3945.79 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}
MAX_PAGESIZE = 30
MAX_RELOAD_TIMES = 5
RESPONSE_TIMEOUT = 10


def __filter_illegal_filename(filename):
    illegal_char = {
        ' ': '',
        '*': '',
        '/': '-',
        '\\': '-',
        ':': '-',
        '?': '-',
        '"': '',
        '<': '',
        '>': '',
        '|': '',
        '－': '-',
        '—': '-',
        '（': '(',
        '）': ')',
        'Ａ': 'A',
        'Ｂ': 'B',
        'Ｈ': 'H',
        '，': ',',
        '。': '.',
        '：': '-',
        '！': '_',
        '？': '-',
        '“': '"',
        '”': '"',
        '‘': '',
        '’': ''
    }
    for item in illegal_char.items():
        filename = filename.replace(item[0], item[1])
    return filename


def get_response(page_num, start_date, end_date, is_total_record_num=False):
    query = {'pageNum': page_num,
             'pageSize': MAX_PAGESIZE,
             'column': 'szse',
             'tabName': 'fulltext',
             'plate': '',
             'stock': '',
             'searchkey': '社会责任',
             'category': '',
             'seDate': start_date + '~' + end_date,
             'sortName': '',
             'sortType': '',
             'isHLtitle': 'true'
             }
    result_info_list = list()
    while True:
        try:
            r = requests.post(QUERY_URL, query, HEADER)
        except Exception as e:
            print(e)
            continue
        if r.status_code == requests.codes.ok and r.text != '':
            break
    my_query = r.json()
    try:
        r.close()
    except Exception as e:
        print(e)

    if is_total_record_num:
        return int(my_query['totalRecordNum'])
    else:
        if int(my_query['totalRecordNum']) == 0:
            return result_info_list
        for each in my_query['announcements']:
            file_link = 'http://static.cninfo.com.cn/' + str(each['adjunctUrl'])
            announcement_date = datetime.datetime.fromtimestamp(int(each['announcementTime']) / 1000)
            title = __filter_illegal_filename(str(each['announcementTitle']))
            result_info_list.append([title, announcement_date, str(each['secCode']), str(each['secName']), file_link])
        return result_info_list


if __name__ == '__main__':
    output_dir = os.path.join('..', 'result')
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    result_dfs = list()
    for year in (2022, 2023):
        for month in range(1, 13):
            end_day = calendar.monthrange(year, month)[1]
            start_date = '{}-{:02d}-01'.format(year, month)
            end_date = '{}-{:02d}-{}'.format(year, month, end_day)
            page_num = 1
            record_num = get_response(page_num, start_date, end_date, True)
            while record_num > 0:
                result_list = get_response(page_num, start_date, end_date)
                result_dfs.append(DataFrame(result_list, columns=[
                    'AnnouncementTitle', 'AnnouncementDate', 'SecCode', 'SecName', 'FileLink']))
                time.sleep(1)
                record_num -= 30
                page_num += 1

    all_link = pd.concat(result_dfs, axis=0)
    all_link.loc[:, 'AnnouncementTitle'] = all_link['AnnouncementTitle'].str.replace('em', '')
    all_link.loc[:, 'AnnouncementTitle'] = all_link['AnnouncementTitle'].str.replace('-', '')
    all_link.reset_index(drop=True, inplace=True)

    all_link.to_csv(os.path.join(output_dir, '20230817_social_responsibility_report.csv'), index=False)

    for i in tqdm(all_link.index):
        url = all_link.loc[i, 'FileLink']
        year = all_link.loc[i, 'AnnouncementDate'].year
        if not os.path.isdir(os.path.join(output_dir, str(year))):
            os.makedirs(os.path.join(output_dir, str(year)))
        save_path = os.path.join(output_dir, str(year),
                                 '{}_{}_{}.pdf'.format(all_link.loc[i, 'AnnouncementDate'].strftime('%Y%m%d'),
                                                       all_link.loc[i, 'SecCode'],
                                                       all_link.loc[i, 'AnnouncementTitle']))
        if os.path.isfile(save_path):
            continue
        pdf = requests.get(url, stream=True)
        with open(save_path, 'wb') as fd:
            for y in pdf.iter_content(102400):
                fd.write(y)
        time.sleep(1)
