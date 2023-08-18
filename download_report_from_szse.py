#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Filename: download_report_from_szse
# @Date: 2023/8/18
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import re
import os
import time
import json
import random
import calendar
import requests

from tqdm.notebook import tqdm
import pandas as pd
from pandas import DataFrame

QUERY_URL = 'http://www.szse.cn/api/disc/announcement/annList?random='

HEADER_DICT = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                  ' Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.203',
    'Referer': 'http://www.szse.cn/disclosure/listed/notice/index.html',
    'Host': 'www.szse.cn',
    'Origin': 'http://www.szse.cn',
    'Content-Type': 'application/json',
    'X-Request-Type': 'ajax',
    'X-Requested-With': 'XMLHttpRequest',
    'Proxy-Connection': 'keep-alive'
}


def get_response(start_date, end_date):
    query = {'channelCode': ["listedNotice_disc"],
             'pageNum': 1,
             'pageSize': 50,
             'seDate': [start_date, end_date],
             'searchKey': ["社会责任"],
             }
    result_info_list = list()

    r = requests.post(QUERY_URL + str(random.random()), json.dumps(query), headers=HEADER_DICT)
    my_query = r.json()
    try:
        r.close()
    except Exception as e:
        print(e)

    total_num = my_query['announceCount']
    page_num = 1
    report_info_list = list()

    while True:
        data_list = my_query['data']
        for report_item in data_list:
            title = report_item['title'].split('：')[-1]
            report_info_list.append(
                [report_item['secCode'][0], report_item['secName'][0], report_item['publishTime'], title,
                 'http://disc.static.szse.cn/download{}'.format(report_item['attachPath'])])
        total_num -= 50
        if total_num <= 0:
            break
        page_num += 1
        query['pageNum'] = page_num
        r = requests.post(QUERY_URL + str(random.random()), json.dumps(query), headers=HEADER_DICT)
        my_query = r.json()
        try:
            r.close()
        except Exception as e:
            print(e)

    return report_info_list


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
            result_list = get_response(start_date, end_date)
            result_dfs.append(DataFrame(result_list, columns=[
                'SecCode', 'SecName', 'AnnouncementDate', 'AnnouncementTitle', 'FileLink']))
            time.sleep(1)
    szse_report_df = pd.concat(result_dfs, axis=0).reset_index(drop=True)
    szse_report_df.loc[:, 'AnnouncementDate'] = pd.to_datetime(szse_report_df['AnnouncementDate'])

    szse_report_df.to_csv(os.path.join(output_dir, '20230818_social_responsibility_report_szse.csv'), index=False)
    szse_report_df.to_pickle(os.path.join(output_dir, '20230818_social_responsibility_report_szse.pkl'))

    cninfo_df: DataFrame = pd.read_pickle(os.path.join(output_dir, '20230817_social_responsibility_report.pkl'))

    for i in tqdm(szse_report_df.index):
        seccode = szse_report_df.loc[i, 'SecCode']
        ann_date = szse_report_df.loc[i, 'AnnouncementDate']
        tmp_cninfo_df: DataFrame = cninfo_df.loc[(cninfo_df['SecCode'] == seccode) &
                                                 (cninfo_df['AnnouncementDate'] == ann_date)].copy()
        if not tmp_cninfo_df.empty:
            continue

        print(seccode, ann_date)

        url = szse_report_df.loc[i, 'FileLink']
        save_path = os.path.join(output_dir, str(ann_date.year),
                                 '{}_{}_{}.pdf'.format(szse_report_df.loc[i, 'AnnouncementDate'].strftime('%Y%m%d'),
                                                       szse_report_df.loc[i, 'SecCode'],
                                                       szse_report_df.loc[i, 'AnnouncementTitle']))
        pdf = requests.get(url, stream=True, headers=HEADER_DICT)
        with open(save_path, 'wb') as fd:
            for y in pdf.iter_content(102400):
                fd.write(y)
        time.sleep(1)
