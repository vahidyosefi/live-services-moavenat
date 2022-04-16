# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 15:09:46 2021

@author: vahid
"""

import pandas as pd
import numpy as np
import datetime
import os
import time
import pyodbc
from pyodbc import *
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine


start = time.time() 

# df0 = pd.DataFrame()
# 
df_shima = pd.DataFrame()

# del df_shima 

#################################################################     Ekhtesasi

# EPG_ekhtesasi_00= pd.read_excel(r'D:\python\EPG_vahid\progress\merge\match merge\ekhtesasi.xlsx')
# EPG_ekhtesasi_00= pd.read_csv(r'D:\back up\EPG\14001018\EPG_ekhtesasi_.csv')


connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="Vahid01")
cursor = connection.cursor()
EPG_ekhtesasi_0= psql.read_sql('select * from public."EPG_Ekhtesasi"', connection)


# EPG_ekhtesasi_0.to_excel(r'D:\final_EPG_powerbi\power_bi_total\Shima\EPG_ekhtesasi_0.xlsx', index=False)

print('فراخونی اختصاصی')

EPG_ekhtesasi_0 = EPG_ekhtesasi_0.rename(columns = {"نام شبکه":"chan"})
EPG_ekhtesasi_0 = EPG_ekhtesasi_0.drop([], axis=1)
EPG_ekhtesasi_0.reset_index()

EPG_ekhtesasi_0['chan'] = EPG_ekhtesasi_0['chan'].str.strip()

df_shima_0 = EPG_ekhtesasi_0.query(" chan == 'جام'")
df_shima_1 = EPG_ekhtesasi_0.query(" chan == 'نما'")
df_shima_2 = EPG_ekhtesasi_0.query(" chan == 'نوا'")
df_shima_3 = EPG_ekhtesasi_0.query(" chan == 'آرا'")
df_shima_4 = EPG_ekhtesasi_0.query(" chan == 'حبیب'")
df_shima_5 = EPG_ekhtesasi_0.query(" chan == 'جوانه'")
df_shima_6 = EPG_ekhtesasi_0.query(" chan == 'لبیک'")
# df_shima_7 = EPG_ekhtesasi_0.query(" chan == 'جام'")
# df_shima_8 = EPG_ekhtesasi_0.query(" chan == 'جام'")
# df_shima_9 = EPG_ekhtesasi_0.query(" chan == 'جام'")



df_shima_00 = [df_shima_0, df_shima_1, df_shima_2, df_shima_3 , df_shima_4, df_shima_5, df_shima_6]
df_shima = pd.concat(df_shima_00)
# df_shima = df_shima_6

df_shima = df_shima.rename(columns = {"chan":"نام شبکه"})


engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Vahid01',pool_size=20, max_overflow=100,)
con=engine.connect()

df_shima.to_sql('shima',con,if_exists='replace', index=False)

print('Write shima in Postgres SQL')


# date



end = time.time()
# total time taken
mo = (end - start)/60
print ('مدت زمان اجرا برنامه به دقیقه',mo)
print(f"Runtime of the program is {end - start}")




