# coding: utf-8
#連関構造推計用プログラム

import pandas as pd
import csv
import assosiate2
from multiprocessing import Pool
from multiprocessing import Process
import gc

print('now, data importintg....')
data = pd.read_csv('2016-10-12TRD2011TSCA_weight_fitted.csv')
#data = pd.read_csv('test.csv')
data  = data[0:1000]

in_goods = data['new_HNMKM_KJ']
in_H_city = data['hc_j5_cd']
in_Z_city = data['jc_j5_cd']
in_value = data['weight_fitted']
in_h_cd = data['HC_KG_CD']
in_z_cd = data['JC_KG_CD']
in_h_ind = data['hc_KG_SGY_CD']
in_z_ind = data['jc_KG_SGY_CD']


#値のない行を削除
print('Ejecting error values....')
index = []
for i in range(len(in_goods)):
    if in_goods[i] == in_goods[i] and in_H_city[i] == in_H_city[i] and in_Z_city[i] == in_Z_city[i] and in_value[i] == in_value[i] and in_h_cd[i] == in_h_cd[i]:
        index.append(i)

print('indexされたリストを作成中...')


list_to_index = [in_goods,in_H_city,in_Z_city,in_value,in_h_cd,in_z_cd, in_z_ind,in_h_ind]
indexed_list = assosiate2.multi_index_list(list_to_index, index)

goods = indexed_list[0]
H_city = indexed_list[1]
Z_city = indexed_list[2] 
value = indexed_list[3]
h_cd = indexed_list[4]
z_cd = indexed_list[5]
z_ind = indexed_list[6]
h_ind = indexed_list[7]
z_cd_list = list(set(z_cd)) #受注業者の集合

del in_goods, in_H_city, in_Z_city, in_value, in_h_cd, in_z_cd, in_z_ind, in_h_ind
gc.collect()

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


print('データ型を変更中...')
columns = ['Input_area','Output_area','Input_industory','Output_industry','value','ind_pair']

output = trans_city_value_goodsSet_rate[0][:]

del trans_city_value_goodsSet_rate
gc.collect()

output_data = pd.DataFrame(output, columns = columns)

del output
gc.collect()

print('csvを出力中....')
output_data.to_csv('2016-10-12市区郡別_産業連関精算表_2014_重複あり_受注なし企業なし.csv')
#output_data.to_csv('test_result.csv')
print('completed...!!')
