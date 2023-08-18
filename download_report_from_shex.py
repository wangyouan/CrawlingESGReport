#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: download_report_from_shex
# @Date: 2023/8/18
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import re
import os
import time
import json
import calendar
import requests

from tqdm.notebook import tqdm
import pandas as pd
from pandas import DataFrame

QUERY_URL = 'http://query.sse.com.cn/security/stock/queryCompanyBulletinNew.do'

HEADER_DICT = {
    'Cookie': "ba17301551dcbaf9_gdp_user_key=; gdp_user_id=gioenc-a435age6%2C22g5%2C57ae%2C8529%2C35be1g7a2690; "
              "ba17301551dcbaf9_gdp_session_id_504af8dd-0dab-4a19-ac79-32f19248ea7d=true; ba17301551dcbaf9_gdp_"
              "session_id=1f977167-c2a1-4943-ab51-6695017424c5; ba17301551dcbaf9_gdp_session_id_1f977167-c2a1-4943"
              "-ab51-6695017424c5=true; ba17301551dcbaf9_gdp_sequence_ids={%22globalKey%22:139%2C%22VISIT%22:3%"
              "2C%22PAGE%22:7%2C%22CUSTOM%22:22%2C%22VIEW_CLICK%22:108%2C%22VIEW_CHANGE%22:3}",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                  ' Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.203',
    'Referer': 'http://www.sse.com.cn/'
}


def get_linkage_to_ESG_report(start_date, end_date):
    params = {'jsonCallBack': 'jsonpCallback88284474',
              'isPagination': 'true',
              'pageHelp.pageSize': 25,
              'pageHelp.cacheSize': 1,
              'START_DATE': start_date,
              'END_DATE': end_date,
              'SECURITY_CODE': '',
              'TITLE': '社会责任',
              'BULLETIN_TYPE': '',
              'stockType': '',
              'pageHelp.pageNo': 1,
              'pageHelp.beginPage': 1,
              'pageHelp.endPage': 1,
              '_': int(time.time() * 1000)
              }
    req = requests.get(QUERY_URL, params=params, headers=HEADER_DICT)
    raw_json = re.split('Callback\d+\(', req.text)[-1][:-1]
    info_json = json.loads(raw_json)['pageHelp']
    req.close()
    total_record_num = info_json['total']
    report_info_list = list()
    page_num = 1
    while True:
        data_list = info_json['data']
        for report_list in data_list:
            for report_item in report_list:
                title = report_item['TITLE']
                if '社会责任' not in title:
                    continue

                report_info_list.append(
                    [report_item['SECURITY_CODE'], report_item['SECURITY_NAME'], report_item['SSEDATE'], title,
                     'http://www.sse.com.cn{}'.format(report_item['URL'])])

        page_num += 1
        total_record_num -= 25
        if total_record_num <= 0:
            break
        params['pageHelp.pageNo'] = page_num
        params['pageHelp.beginPage'] = page_num
        params['pageHelp.endPage'] = page_num
        params['_'] = int(time.time() * 1000)
        req = requests.get(QUERY_URL, params=params, headers=HEADER_DICT)
        raw_json = re.split('Callback\d+\(', req.text)[-1][:-1]
        info_json = json.loads(raw_json)['pageHelp']
        req.close()

    return report_info_list


if __name__ == '__main__':
    output_dir = os.path.join('..', 'result')

    result_dfs = list()
    for year in (2022, 2023):
        for month in range(1, 13):
            end_day = calendar.monthrange(year, month)[1]
            start_date = '{}-{:02d}-01'.format(year, month)
            end_date = '{}-{:02d}-{}'.format(year, month, end_day)
            result_list = get_linkage_to_ESG_report(start_date, end_date)
            result_dfs.append(DataFrame(result_list, columns=[
                'SecCode', 'SecName', 'AnnouncementDate', 'AnnouncementTitle', 'FileLink']))
            time.sleep(1)
    sse_report_df = pd.concat(result_dfs, axis=0).reset_index(drop=True)
    sse_report_df.to_csv(os.path.join(output_dir, '20230818_social_responsibility_report_sse.csv'), index=False)
    sse_report_df.to_pickle(os.path.join(output_dir, '20230818_social_responsibility_report_sse.pkl'))
    sse_report_df.loc[:, 'AnnouncementDate'] = pd.to_datetime(sse_report_df['AnnouncementDate'], format='%Y-%m-%d')

    cninfo_df: DataFrame = pd.read_pickle(os.path.join(output_dir, '20230817_social_responsibility_report.pkl'))

    for i in tqdm(sse_report_df.index):
        seccode = sse_report_df.loc[i, 'SecCode']
        ann_date = sse_report_df.loc[i, 'AnnouncementDate']
        tmp_cninfo_df: DataFrame = cninfo_df.loc[(cninfo_df['SecCode'] == seccode) &
                                                 (cninfo_df['AnnouncementDate'] == ann_date)].copy()
        if not tmp_cninfo_df.empty:
            continue

        print(seccode, ann_date)

        url = sse_report_df.loc[i, 'FileLink']
        save_path = os.path.join(output_dir, str(ann_date.year),
                                 '{}_{}_{}.pdf'.format(sse_report_df.loc[i, 'AnnouncementDate'].strftime('%Y%m%d'),
                                                       sse_report_df.loc[i, 'SecCode'],
                                                       sse_report_df.loc[i, 'AnnouncementTitle']))
        pdf = requests.get(url, stream=True, headers=HEADER_DICT)
        with open(save_path, 'wb') as fd:
            for y in pdf.iter_content(102400):
                fd.write(y)
        time.sleep(1)
