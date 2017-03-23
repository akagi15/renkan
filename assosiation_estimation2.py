# coding: utf-8
#連関構造推計用プログラム

from __future__ import print_function
from multiprocessing import Pool
from multiprocessing import Process
import pandas as pd
import csv
import assosiate2
import gc
import sys
import numpy as np
from numba.decorators import jit
import time
start = time.time()

proc = 6 #並列計算のproc数

print('now, data importintg....')
#data = pd.read_csv('../test.csv') #テスト用
data = pd.read_csv('test.csv')
data = data.loc[0:1000,['new_HNMKM_KJ','hc_j5_cd','jc_j5_cd','weight_fitted','HC_KG_CD','JC_KG_CD','hc_KG_SGY_CD','jc_KG_SGY_CD']]
#欠損値の除去
data = data.dropna()
#indexの振り直し
data = data.reset_index(drop=True)

goods = list(data['new_HNMKM_KJ'])
H_city = list(data['hc_j5_cd'])
Z_city = list(data['jc_j5_cd'])
value = list(data['weight_fitted'])
h_cd = list(data['HC_KG_CD'])
z_cd = list(data['JC_KG_CD'])
h_ind = list(data['hc_KG_SGY_CD'])
z_ind = list(data['jc_KG_SGY_CD'])

del data
gc.collect()
z_cd_list = list(set(z_cd)) #受注業者の集合


#受注企業毎の取引品目及び取引額の組を返す
cd_trans = assosiate2.multi_get_index_cd_trans(z_cd,z_cd_list,value, goods)

#取引品目の集合とそれぞれに対する割合を返す
print('割合を計算開始')
cd_trans_rate = assosiate2.multi_get_rate(cd_trans)

del cd_trans
gc.collect()
  
trans_city_value_goodsSet_rate = []

#各取引毎のデータを作成
print('出力データを作成中....')
trans_city_value_goodsSet_rate = assosiate2.multi_make_data(h_cd, z_cd, goods, cd_trans_rate, Z_city, H_city, value, z_ind, h_ind)


del cd_trans_rate, h_cd, z_cd, goods, Z_city, H_city, value
gc.collect()

only_h_trans = trans_city_value_goodsSet_rate[1][:] #受注していない発注企業

#受注していない企業を除いた取引のまとめのみを出力
print('データ型を変更中...')
columns = ['Input_area','Output_area','Input_industory','Output_industry','Value','Ind_pair']

output = trans_city_value_goodsSet_rate[0][:]

del trans_city_value_goodsSet_rate
gc.collect()

output_data = pd.DataFrame(output, columns = columns)
only_h_trans_data = pd.DataFrame(only_h_trans,columns = columns)
del output
gc.collect ()


print('temp.csvを出力中....')
output_data.to_csv('../renkan_temp.csv')
only_h_trans_data.to_csv('../only_h_trans_temp.csv')

#受注していない企業対策
#--業種の組み合わせ毎の品目の組み合わせ毎の取引額の平均割合を作成する
#----dataをソート
output_data = output_data.sort_values(by = ["Ind_pair","Input_industory","Output_industry"],ascending=True)
output_data = output_data.reset_index(drop=True)

print('業種組み合わせ作成中')
ind_pair_goods_pair_value_list = assosiate2.make_ind_pair_goods_pair_value_list(output_data)

#受注していない発注企業分の取引データを作成する
print('非受注企業の取引データ作成中')
only_h_trans_value = assosiate2.multi_make_only_h_trans_value(only_h_trans,ind_pair_goods_pair_value_list)

only_h_trans_value_data = pd.DataFrame(only_h_trans_value, columns = columns)

del only_h_trans_value
gc.collect()

output_data = output_data.append(only_h_trans_value_data)

del only_h_trans_value_data
gc.collect()

#重複した組み合わせを削除
print('重複組合せの統合中')
output_data = output_data.sort_values(by = ["Input_area","Output_area","Input_industory","Output_industry"],ascending=True)
output_data = output_data.reset_index(drop=True)


output_data = assosiate2.integrate(output_data)
print('output.csvを出力中....')
output_data.to_csv('../output.csv')
temp_length = len(list(output_data["Input_area"]))

elapsed_time = str(time.time() - start)
print(str(temp_length) + 'records, completed...!! it takes ' + elapsed_time + '[sec]' )
