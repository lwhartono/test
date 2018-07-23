# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 17:45:34 2018

This script retrieves flash deal product stock amount from mothership
in a minute by minute basis.

@author: Leonard Hartono
"""

import queryBL as qbl
import pandas as pd
import math

from sys import argv
from datetime import datetime, timedelta

time_execution = argv[1]

current_time = datetime.now()
print('start_query: ', current_time)

query_time = current_time.strftime("'%Y-%m-%d %H:%M:00'")

# get last data from influxdb
q_influx = '''
SELECT *
FROM flashdeal_stock
ORDER BY time DESC
LIMIT 1
'''
stock_history = qbl.queryI(q_influx)

if stock_history.empty:
    last_import = ''
else:
    stock_history['time'] = pd.to_datetime(stock_history['time'])
    last_import = stock_history.iloc[0]['time'].strftime("'%Y-%m-%d %H:%M:00'")
    last_import = last_import + ' - INTERVAL 7 hour'

# list of all queries
# get flashdeal data from 7 days ago to today
q_stock_all = '''
SELECT
  ddc.start_date + INTERVAL 7 hour AS start_date,
  ddc.end_date + INTERVAL 7 hour AS end_date,
  ddc.name AS campaign_name,
  SUM(ddp.stock) AS beginning_stock
FROM
  promo_daily_deal_products ddp
  JOIN promo_daily_deal_campaigns ddc ON ddc.id = ddp.campaign_id
WHERE
  ddc.start_date >= ''' + query_time + ''' - INTERVAL 7 day - INTERVAL 7 hour
  AND ddc.start_date <= ''' + query_time + '''
GROUP BY
  ddc.name
ORDER BY
  ddc.start_date DESC
    '''
# get all flashdeal data from 7 days ago to today
q_transaction_all = '''
SELECT
  pt.created_at + INTERVAL 7 hour AS created_at,
  ddc.name,
  ddt.quantity,
  ddt.status
FROM
  promo_daily_deal_transactions ddt
  JOIN promo_daily_deal_products ddp ON ddp.id = ddt.product_id
  JOIN promo_daily_deal_campaigns ddc ON ddc.id = ddp.campaign_id
  JOIN payment_transactions pt ON pt.id = ddt.transaction_id
WHERE
  pt.created_at >= ''' + query_time + ''' - INTERVAL 7 day - INTERVAL 7 hour
  AND (ddt.status = 'transaction' OR ddt.status = 'paid')
'''
# get flashdeal stock data since last export
q_stock = '''
SELECT
  ddc.start_date + INTERVAL 7 hour AS start_date,
  ddc.end_date + INTERVAL 7 hour AS end_date,
  ddc.name AS campaign_name,
  SUM(ddp.stock) AS beginning_stock
FROM
  promo_daily_deal_products ddp
  JOIN promo_daily_deal_campaigns ddc ON ddc.id = ddp.campaign_id
WHERE
  (ddc.start_date <= ''' + last_import + '''
  AND ddc.end_date >= ''' + last_import + ''')
  OR (ddc.start_date >= ''' + last_import + '''
  AND ddc.end_date <= ''' + query_time + ''' - INTERVAL 7 hour)
GROUP BY
  ddc.name
'''
# get flashdeal transaction data since last export
q_transaction = '''
SELECT
  pt.created_at + INTERVAL 7 hour AS created_at,
  ddc.name,
  ddt.quantity,
  ddt.status
FROM
  promo_daily_deal_transactions ddt
  JOIN promo_daily_deal_products ddp ON ddp.id = ddt.product_id
  JOIN promo_daily_deal_campaigns ddc ON ddc.id = ddp.campaign_id
  JOIN payment_transactions pt ON pt.id = ddt.transaction_id
WHERE
  ddc.name IN
  (
    SELECT * FROM
    (
      SELECT DISTINCT name
      FROM promo_daily_deal_campaigns
      WHERE
        end_date >= ''' + last_import + ''' - INTERVAL 7 hour
        AND start_date <= ''' + query_time + ''' - INTERVAL 7 hour
    ) AS campaign
  )
  AND (ddt.status = 'transaction' OR ddt.status = 'paid')
'''

# create function to calculate flashdeal stock on minute by minute basis
def get_and_append_stock_data(stock_data, transaction_data, last_export, current_time):
    time = []
    stock = []
    for index, current_stock_data in stock_data.iterrows():
        if (current_stock_data['end_date'] < last_export or current_stock_data['start_date'] > current_time):
            continue
        elif current_stock_data['end_date'] < current_time:
            delta = current_stock_data['end_date'] - current_stock_data['start_date']
            start_time = current_stock_data['start_date']
            transaction_time = start_time
        elif current_stock_data['start_date'] < last_export:
            delta = current_time - last_export
            start_time = last_export
            transaction_time = current_stock_data['start_date']
        else:
            delta = current_time - current_stock_data['start_date']
            start_time = current_stock_data['start_date']
            transaction_time = start_time
        delta_minutes = math.floor(delta.total_seconds()/60)
        current_stock = current_stock_data['beginning_stock']
        for j in range(delta_minutes):
            t = start_time + timedelta(minutes = j + 1)
            sold = transaction_data['quantity'][(transaction_data['created_at'] >= transaction_time) \
                                   & (transaction_data['created_at'] <= t)].sum()
            s = current_stock - sold
            time.append(t)
            stock.append(s)
  
    df = pd.DataFrame(
    {'time': time,
     'stock': stock})

    df = df.sort_values(by=['time'], ascending=False)
    df = df.reset_index(drop=True)
    qbl.writeI(df, measurement='flashdeal_stock', time='time', tags='stock', fields=['stock'])
    
# start calculating flashdeal stock on minute by minute basis
if stock_history.empty:
    stock_data = qbl.queryM(q_stock_all)
    transaction_data = qbl.queryM(q_transaction_all)    
    last_export = current_time - timedelta(days=7)   #if data is empty (first write) get data for last 7 days
    get_and_append_stock_data(stock_data, transaction_data, last_export, current_time)

else:
    stock_data = qbl.queryM(q_stock)        
    transaction_data = qbl.queryM(q_transaction)   
    last_export = stock_history['time'].max()
    get_and_append_stock_data(stock_data, transaction_data, last_export, current_time)
    
# =============================================================================
#     delta = current_time - last_export
#     delta_minutes = math.floor(delta.total_seconds()/60)
#     for i in range(delta_minutes):
#         t = last_export + timedelta(minutes = i+1)
#         current_stock = stock_data['beginning_stock'][(stock_data['start_date'] <= current_time) \
#                            & (stock_data['end_date'] >= current_time)].sum()
#         sold = transaction_data['quantity'][transaction_data['created_at'] <= t].sum()
#         s = current_stock - sold
#         time.append(t)
#         stock.append(s)
# =============================================================================
    


