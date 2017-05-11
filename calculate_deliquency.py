# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 15:32:24 2017

@author: linzx
"""

# -*- coding: utf-8 -*-
from __future__ import division, print_function
import numpy as np
import pandas as pd
import mysql.connector

def clean_time(time_series):
    time_series = time_series.values.astype('<M8[D]')
    return time_series

def get_data():
    #连接数据库
    conn = mysql.connector.connect(host='18.16.100.130', user='personnel', passwd='personnel', db='ml')
    cursor = conn.cursor()
    #拿出来的数据已经按照order_id, order_number排好顺序
    cursor.execute("SELECT rp.order_id, rp.plan_id, rp.order_number, rp.end_date, \
    rp.last_date, rp.repayment_status, t.product_id \
    FROM tb_order_repayment_plan AS rp \
    LEFT JOIN tb_order AS t ON rp.order_id = t.order_id \
    ORDER BY rp.order_id, rp.order_number")
    column_names = [x[0] for x in cursor.description]
    #将拿出来的数据以data作为标签
    data = pd.DataFrame(cursor.fetchall(), columns=column_names)
    #下面5笔订单尚未补全，因此先做删除处理 // 后面追加2笔订单
    drop_five = ['JSY2014032700000601', 'JSY2013083000000201', 'JSY2014092900000701', 'JSY2014123000001301', 'JSY2015091400000301', 'JSY2015011200000501', 'JSY20161122000064']
    data = data[~data.order_id.isin(drop_five)]
    order_ids = data['order_id'].unique()
    return data, order_ids

'''has_null = pd.DataFrame(data.groupby(by='order_id').apply(lambda group: group['last_date'].isnull().any()))
has_null.reset_index(level=['order_id'], inplace=True)
order_id_hasnull = has_null[has_null[0] == True]['order_id']

no_null = pd.DataFrame(data.groupby(by='order_id').apply(lambda group: ~group['last_date'].isnull().any()))
no_null.reset_index(level=['order_id'], inplace=True)
order_id_notnull = no_null[no_null[0] == True]['order_id']'''

def get_deli_days(data, ids, ob_period):
    test_slice = data.loc[data.order_id.isin(ids)].copy()
    cut_date = pd.DataFrame(test_slice.groupby('order_id').apply(lambda group: group['end_date'].iloc[0] + pd.Timedelta(ob_period, 'D')))
    cut_date.reset_index(level=['order_id'], inplace=True)
    test_slice = test_slice.merge(cut_date, how='left', on='order_id')
    test_slice.rename(columns={0:'cut_date'}, inplace=True)
    test_slice = test_slice[test_slice['cut_date'] <= pd.to_datetime('today')]
    test_slice.loc[test_slice['last_date'].isnull(), 'last_date'] = test_slice['cut_date']
    
    #test_slice = test_slice[test_slice['order_id'].isin(['JSY2013123100000301', 'JSY2014102800000401'])]
    test_slice = test_slice[test_slice['order_id'].isin(ids)]
    test_slice.loc[:, 'last_date'], test_slice.loc[:, 'end_date'] = clean_time(test_slice.loc[:, 'last_date']), clean_time(test_slice.loc[:, 'end_date'])
    test_slice['last_date_shift'] = test_slice.loc[:, 'last_date'].shift(1)
    temp = pd.DataFrame(test_slice.groupby(by='order_id')['end_date'].min())
    temp.reset_index(level=['order_id'], inplace=True)
    test_slice = test_slice.merge(temp, how='left', on='order_id')
    test_slice.loc[test_slice.end_date_x == test_slice.end_date_y, 'last_date_shift'] = pd.NaT
    test_slice.rename(columns={'end_date_x':'end_date'}, inplace=True)
    test_slice['comparison'] = test_slice.end_date.values > test_slice.last_date_shift.values
    #第一项改为0
    test_slice.loc[test_slice.end_date == test_slice.end_date_y, 'comparison'] = 0
    test_slice.comparison = test_slice.comparison.astype(int)
    
    cumsum = pd.DataFrame(test_slice.groupby('order_id').apply(lambda group: np.cumsum(group['comparison'])))
    cumsum.reset_index(level=['order_id'], inplace=True) 
    cumsum = cumsum.rename(columns={'comparison':'cumsum'})
    
    test_slice['cum_sum'] = cumsum['cumsum']
    groupby_testslice = pd.DataFrame(test_slice.groupby(by=['order_id', 'cum_sum']).aggregate({'last_date':'max', 'end_date':'min', 'end_date_y':'max'}))
    groupby_testslice.reset_index(level=['cum_sum', 'order_id'], inplace=True)
    
    cut_date = groupby_testslice.end_date_y + pd.Timedelta(ob_period, 'D')
    groupby_testslice['cut_date'] = cut_date
    insert_list = groupby_testslice.cut_date.between(groupby_testslice.end_date, groupby_testslice.last_date)
    
    groupby_testslice['insert_list'] = insert_list
                     
    test_list = pd.DataFrame(groupby_testslice.groupby(by='order_id').apply(lambda group: group['insert_list'].any()))
    test_list.reset_index(level=['order_id'], inplace=True)
    
    matrix1 = groupby_testslice[groupby_testslice['order_id'].isin(test_list[test_list[0] == True]['order_id'])].copy()
    matrix2 = groupby_testslice[groupby_testslice['order_id'].isin(test_list[test_list[0] == False]['order_id'])].copy()
    
    matrix1.loc[matrix1['insert_list'], 'last_date'] = matrix1.loc[matrix1['insert_list'], 'cut_date']
    #matrix = matrix[:insert_list[insert_list == True].index.values[0] + 1]
    matrix1['diff'] = matrix1.last_date - matrix1.end_date
    matrix1_deli = matrix1.groupby(by='order_id').apply(lambda group: group.loc[:(group[group['last_date'] == group['cut_date']].index.values[0])]['diff'].max())
    
    matrix2.loc[:, 'diff'] = matrix2.loc[:, 'last_date'] - matrix2.loc[:, 'end_date']
    matrix2_deli = matrix2.groupby(by='order_id').apply(lambda group: group[group.last_date < group.cut_date]['diff'].max())
    
    result = pd.DataFrame(matrix1_deli.append(matrix2_deli))
    result.reset_index(level=['order_id'], inplace=True)
    result = result.rename(columns={0:'deli_days'})
    result['deli_days'] = result['deli_days'].dt.days
    
    return result
