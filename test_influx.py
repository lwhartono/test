# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 20:55:39 2018

@author: Bukalapak
"""

import queryBL as qbl
import pandas as pd
import math

from datetime import datetime

q_influx = '''
SELECT *
FROM flashdeal_stock
ORDER BY time DESC
'''

stock_history = qbl.queryI(q_influx)

if stock_history.empty:
    print('stock history is empty')
else:
    print('stock history is not empty')
    print(stock_history.head())
    
#qbl.dropI(measurement='flashdeal_stock')

# =============================================================================
# if influx_db.empty:
#     last_export = datetime(2018,7,20,18)
# else:
#     last_export = influx_db.iloc[0]['time']
# 
# current_time = datetime.now()
# delta = current_time - last_export
# delta_minutes = math.floor(delta.total_seconds()/60)
# =============================================================================
