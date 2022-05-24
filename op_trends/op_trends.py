# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 18:14:35 2021

@author: Zly_admin
"""
import pandas as pd                        
from pytrends.request import TrendReq
import pytrends
import pytz
import datetime as dt
import pyodbc
import sqlalchemy

pd.set_option('display.max_columns', 500)
pd.set_option('display.expand_frame_repr', False)

pytz.all_timezones
pytrend = TrendReq(hl='pl-PL', tz=60)

#list_trends = ['kolczyki','kolczyki dla dzieci','kolczyki dla dziewczynek','kolczyki do pępka','kolczyki srebrne']

relatedd = [['kolczyki'],['naszyjnik'],['biżuteria'],['pierscionek']]           

#pytrend.build_payload(list_trends, timeframe='today 5-y', geo='PL', gprop='')

df_related_rising_summ = pd.DataFrame(columns=list(['pop_type','asked_date','asked_topic','value','topic_title','topic_type']))
df_related_top_summ = pd.DataFrame(columns=list(['pop_type','asked_date','asked_topic','value','topic_title','topic_type']))

popularnosc_rising_cnt = 500
popularnosc_top_cnt = 5

for i in relatedd:
    pytrend.build_payload(i, timeframe='today 5-y', geo='PL', gprop='')
    df = pytrend.related_topics()    
    df_top = df[i[0]]['top']
    df_rising = df[i[0]]['rising']
    df_top_max = df_top.loc[(df_top.formattedValue != 'Przebicie') & (df_top.value > popularnosc_top_cnt) & (df_top.value < 100)][['value','topic_title','topic_type']]
    df_rising_max = df_rising.loc[(df_rising.formattedValue != 'Przebicie') & (df_rising.value > popularnosc_rising_cnt)][['value','topic_title','topic_type']]

    df_top_max['asked_topic'] = i*len(df_top_max)
    df_rising_max['asked_topic'] = i*len(df_rising_max)

    df_top_max['asked_date'] = dt.datetime.today().strftime('%Y-%m-%d')
    df_rising_max['asked_date'] = dt.datetime.today().strftime('%Y-%m-%d')
    
    df_top_max['pop_type'] = 'top'
    df_rising_max['pop_type'] = 'rising'

    df_related_rising_summ = df_related_rising_summ.append(df_rising_max)
    df_related_top_summ = df_related_top_summ.append(df_top_max)

print(' \n')
print(df_related_rising_summ)
print(' \n')
print(df_related_top_summ)

##################################################################

df_insert = pd.concat([df_related_rising_summ,df_related_top_summ], ignore_index=True)

nazwa_tab1 = 'op_trends_tab'

database_name = 'ma'
driver_m = 'SQL Server Native Client 11.0'
server_name = 'DESKTOP-0EEPPO3\SQLEXPRESS01'

engine = sqlalchemy.create_engine('mssql+pyodbc://' + server_name + r'/' + database_name + '?driver=' + driver_m + '?Trusted_Connection=yes')
conn_m = engine.connect()

df_existing = pd.read_sql('select * from op_trends_tab', conn_m)

df_insert.value = df_insert.value.astype('int64')

df_insert.asked_date = pd.to_datetime(df_insert.asked_date)
df_existing.asked_date = pd.to_datetime(df_existing.asked_date)

df_insert = df_insert.merge(df_existing, how = 'left', on = [str(x) for x in df_insert.columns], indicator=True,left_index=False, right_index=False)
df_insert = df_insert.loc[df_insert['_merge'] == 'left_only'].iloc[:,:-1]


df_insert.to_sql(nazwa_tab1, engine, if_exists = 'append', index = False, method = None)

print(1)
##################################################################


