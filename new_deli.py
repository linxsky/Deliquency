# -*- coding: utf-8 -*-
from __future__ import division, print_function
from future.builtins import input, range
import numpy as np
import pandas as pd
import mysql.connector
import time
 
def clean_time(time):
    time = time.values.astype('<M8[D]')
    return time

def find_max(time):
    count = 1
    max_time = []
    i = 0
    if len(time) > 1:
      while i < len(time)-1:
        if time[i+1] == time[i] + 1:
           count += 1
           max_time.append(count)
           i += 1
        else:
           count = 1
           max_time.append(count)
           i += 1
    else:
        max_time.append(1)
    max_deli = max(max_time)
    return max_deli

#测试订单：
    #正常还款 JSY2013092300000201
    #提前结清 JSY2015012900001201 JSY2014022800000301
    #逾期 JSY2014121200000501 JSY2014121800000201 JSY2013120900000301
    #最终结清 JSY2013120900000201
    #只有一期 JSY20170122000009
    #还未到还款时间 JSY2016101700000501

#max_deli_1 针对正常还款、提前结清和最终结清的订单
def max_deli_1(order_id, t):
    data_test = data[data.order_id == order_id]
    data_test = data_test.reset_index(drop=True)
    if pd.notnull(data_test.last_date.iloc[0]):
        data_test.last_date = pd.Series(clean_time(data_test.last_date)).fillna(method='ffill')
    else:
        data_test.last_date = pd.Series(clean_time(data_test.last_date)).fillna(method='bfill')
    time_range = [pd.date_range(data_test.end_date.iloc[x], data_test.last_date.iloc[x])[:-1] for x in range(len(data_test))]
    time_union = set().union(*time_range)
    time_union_cleaned = clean_time(pd.Series(list(time_union)).sort_values())
    cut_date = data_test.end_date[0] + pd.Timedelta(t, 'D')
    time_union_cleaned = time_union_cleaned[pd.to_datetime(time_union_cleaned) < cut_date]
    max_deli = find_max(time_union_cleaned)
    return max_deli
#print(max_deli_1('JSY2013120900000201', 1730))

#max_deli_2针对逾期、只有一期和未到还款日的订单
def max_deli_2(order_id, t):
    data_test = data[data.order_id == order_id]
    data_test = data_test.reset_index(drop=True)
    cut_date = data_test.end_date[0] + pd.Timedelta(t, 'D') if data_test.end_date[0] + pd.Timedelta(t, 'D') < pd.to_datetime('today') else pd.to_datetime('today')
    data_test.last_date = pd.Series(clean_time(data_test.last_date)).fillna(value=cut_date)
    time_range = [pd.date_range(data_test.end_date.iloc[x], data_test.last_date.iloc[x])[:-1] for x in range(len(data_test))]
    time_union = set().union(*time_range)
    time_union_cleaned = clean_time(pd.Series(list(time_union)).sort_values())
    max_deli = find_max(time_union_cleaned)
    return max_deli
#print(max_deli_2("JSY2013120900000301", 306))

#每期都有还款的订单
'''data_test = data[data.order_id == ""]
data_test = data_test.reset_index(drop=True)
data_test.last_date = pd.Series(clean_time(data_test.last_date)).fillna(method='ffill')
time_range = [pd.date_range(data_test.end_date.iloc[x], data_test.last_date.iloc[x])[:-1] for x in range(len(data_test))]
time_union = set().union(*time_range)
time_union_cleaned = clean_time(pd.Series(list(time_union)).sort_values())
max_deli = find_max(time_union_cleaned)
print(max_deli)'''

#提前结清、最终结清的订单
'''data_test = data[data.order_id == "JSY2013120900000201"]
data_test = data_test.reset_index(drop=True)
data_test.last_date = pd.Series(clean_time(data_test.last_date)).fillna(method='ffill')
time_range = [pd.date_range(data_test.end_date.iloc[x], data_test.last_date.iloc[x]) for x in range(len(data_test))]
time_union = set().union(*time_range)
time_union_cleaned = clean_time(pd.Series(list(time_union)).sort_values())
max_deli = find_max(time_union_cleaned)
print(max_deli)'''

#逾期未还的订单
'''data_test = data[data.order_id == "JSY2013120900000301"]
data_test = data_test.reset_index(drop=True)
cut_date = data_test.end_date[0] + pd.Timedelta(730, 'D') if data_test.end_date[0] + pd.Timedelta(730, 'D') < pd.to_datetime('today') else pd.to_datetime('today')
data_test.last_date = pd.Series(clean_time(data_test.last_date)).fillna(value=cut_date)
time_range = [pd.date_range(data_test.end_date.iloc[x], data_test.last_date.iloc[x])[:-1] for x in range(len(data_test))]
time_union = set().union(*time_range)
time_union_cleaned = clean_time(pd.Series(list(time_union)).sort_values())
max_deli = find_max(time_union_cleaned)
print(max_deli)'''

def choose_max_deli(t):
    conn = mysql.connector.connect(host='18.16.100.130', user='personnel', passwd='personnel', db='ml')
    cursor = conn.cursor()
    cursor.execute("SELECT rp.order_id, rp.plan_id, rp.order_number, rp.end_date, rp.last_date, rp.repayment_status \
    FROM tb_order_repayment_plan AS rp \
    ORDER BY rp.order_id, rp.order_number")
    column_names = [x[0] for x in cursor.description]
    data = pd.DataFrame(cursor.fetchall(), columns=column_names)
    unique_order_id = pd.Series(data.order_id.unique())
    data_slice = data[data.order_id == order_id]
    if (data_slice.repayment_status <= 0).any():
        return max_deli_2(order_id, t)
    elif (data_slice.repayment_status > 0).all():
        if pd.isnull(data_slice.last_date).all():
            return max_deli_2(order_id, t)
        else:
            return max_deli_1(order_id, t)

start = time.time()
conn = mysql.connector.connect(host='18.16.100.130', user='personnel', passwd='personnel', db='ml')
cursor = conn.cursor()
cursor.execute("SELECT rp.order_id, rp.plan_id, rp.order_number, rp.end_date, rp.last_date, rp.repayment_status \
FROM tb_order_repayment_plan AS rp \
ORDER BY rp.order_id, rp.order_number")
column_names = [x[0] for x in cursor.description]
data = pd.DataFrame(cursor.fetchall(), columns=column_names)
'''cursor.execute("SELECT rp.order_id, rp.plan_id, rp.order_number, em.create_date \
FROM tb_order_repayment_plan AS rp \
LEFT JOIN tb_order_exceed_man AS em ON rp.order_id = em.order_id \
ORDER BY rp.order_id, rp.order_number, em.create_date")
column_names = [x[0] for x in cursor.description]
exceed_man = pd.DataFrame(cursor.fetchall(), columns=column_names)'''
unique_order_id = pd.Series(data.order_id.unique())
t = 360
result = {}

for i in unique_order_id:
    data_slice = data[data.order_id == i]
    if pd.to_datetime(data_slice.end_date.iloc[0]) + pd.Timedelta(360, 'D') < pd.to_datetime('today'):   
        if (data_slice.repayment_status <= 0).any():
            result[i] = max_deli_2(i, t)
        elif (data_slice.repayment_status > 0).all():
            if pd.isnull(data_slice.last_date).all():
                result[i] = max_deli_2(i, t)
            else:
                result[i] = max_deli_1(i, t)
    else:
        pass
end = time.time()
print(end-start)
