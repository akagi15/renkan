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


@jit
def make_ind_pair_goods_pair_value_list(output_ndarray):
    """
    業種組み合わせ,投入商品,産出商品,の順で異なった場合に行が変わる
    業種が同じ総額,産出商品の組み合わせ数を記録し業種が異なった場合に総額から割合を出す
    業種ー投入商品ー産出商品が同じ限り,組み合わせ数を記録し,異なった場合に平均を出す
    """
    ind_pair_data = output_ndarray[:,5].tolist()
    input_ind_data = output_ndarray[:,2].tolist()
    output_ind_data = output_ndarray[:,3].tolist()
    value_data = output_ndarray[:,4].tolist()

    #del output_ndarray
    #gc.collect()

    temp_ind_pair = ind_pair_data[0]
    temp_input_ind = input_ind_data[0]
    temp_output_ind = output_ind_data[0]
    temp_value = value_data[0]
    ind_pair_goods_pair_value_list = [[temp_ind_pair,temp_input_ind,temp_output_ind,temp_value,1]] #[ind_pair,品目,品目,額] 
    out_line_num = 0 #ind_pair_goods_pair_value_list の桁数
    same_input_count = 1
    same_input_total = 0
    for num in  range(len(ind_pair_data)):
        ind_pair = ind_pair_data[num]
        input_ind = input_ind_data[num]
        output_ind = output_ind_data[num]
        value = value_data[num]
        out_line = [] 
        
        if ind_pair != temp_ind_pair:
            out_line.append(ind_pair)
            out_line.append(input_ind)
            out_line.append(output_ind)
            out_line.append(value)
            out_line.append(1)
            ind_pair_goods_pair_value_list.append(out_line)
            ind_pair_goods_pair_value_list[out_line_num][3] /= ind_pair_goods_pair_value_list[out_line_num][4] 

            for i  in range(same_input_count):
                if same_input_total == 0:
                    ind_pair_goods_pair_value_list[out_line_num - i][3] = 1
                else:
                    ind_pair_goods_pair_value_list[out_line_num - i][3] /= same_input_total
            
            out_line_num += 1 
            same_input_count = 1
            same_input_total = value

        else:
            if input_ind != temp_input_ind:
                out_line.append(ind_pair)
                out_line.append(input_ind)
                out_line.append(output_ind)
                out_line.append(value)
                out_line.append(1)
                ind_pair_goods_pair_value_list.append(out_line)
                ind_pair_goods_pair_value_list[out_line_num][3] /= ind_pair_goods_pair_value_list[out_line_num][4] 
                out_line_num += 1
                same_input_total += value
                same_input_count += 1

            elif input_ind == temp_input_ind:

                if output_ind != temp_output_ind:
                    out_line.append(ind_pair)
                    out_line.append(input_ind)
                    out_line.append(output_ind)
                    out_line.append(value)
                    out_line.append(1)
                    ind_pair_goods_pair_value_list.append(out_line)
                    ind_pair_goods_pair_value_list[out_line_num][3] /= ind_pair_goods_pair_value_list[out_line_num][4] 

                    same_input_total += value
                    same_input_count +=1
                    out_line_num += 1

                elif output_ind == temp_output_ind:
                    ind_pair_goods_pair_value_list[out_line_num][3] += value
                    ind_pair_goods_pair_value_list[out_line_num][4] += 1
                    same_input_total += value 

            #最後のデータの場合にvalueを割っておく
            if num == len(list(ind_pair_data)) - 1:
                ind_pair_goods_pair_value_list[out_line_num][3] /= ind_pair_goods_pair_value_list[out_line_num][4] 
                same_input_count += 1
                for i in range(same_input_count):
                    if same_input_total == 0:
                        ind_pair_goods_pair_value_list[out_line_num - i][3] = 1
                    else:
                        ind_pair_goods_pair_value_list[out_line_num - i][3] /= same_input_total
                            
        temp_ind_pair = ind_pair
        temp_output_ind = output_ind
        temp_input_ind = input_ind

    #del temp_ind_pair, temp_input_ind, temp_output_ind, temp_value
    #gc.collect()
    
    return ind_pair_goods_pair_value_list

@jit
def integrate(output_ndarray):
    t0 = output_ndarray[0,0] #temp Input area
    t1 = output_ndarray[0,1] #temp Output area
    t2 = output_ndarray[0,2] #temp Input ind
    t3 = output_ndarray[0,3] #temp Output ind
    t4 = output_ndarray[0,4] #temp value
    t5 = output_ndarray[0,5] #temp ind pair
    temp_length = len(list(output_ndarray[:,0]))
    out_line = [t0,t1,t2,t3,t4,t5]      
    result = [out_line]
    for  num  in range(temp_length):
        #経過出力
        print('now integrating same values...',num,'/', temp_length,'\r', end='')
        sys.stdout.flush()
        data = output_ndarray[num]
        if num != 0:
            t0_ = data[0]
            t1_ = data[1]
            t2_ = data[2]
            t3_ = data[3]
            t4_ = data[4]
            t5_ = data[5]
            if t0 == t0_ and t1 == t1_ and t2 == t2_ and t3 == t3_ :
                out_line[4] += t4_
            else:
                result.append(out_line)
                t0 = t0_
                t1 = t1_
                t2 = t2_
                t3 = t3_
                t4 = t4_
                t5 = t5_
                out_line = [t0,t1,t2,t3,t4,t5]
            
            if num + 1 == temp_length:
                result.append(out_line)
    
    result.pop(0)
    return result

print('now, data importintg....')
#data = pd.read_csv('../test.csv') #テスト用
data = pd.read_csv('../2016-10-12TRD2011TSCA_weight_fitted.csv')
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
output_ndarray = output_data.as_matrix()

print('業種組み合わせ作成中')

ind_pair_goods_pair_value_list = make_ind_pair_goods_pair_value_list(output_ndarray)

del output_ndarray
gc.collect()

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

output_ndarray = output_data.as_matrix() #numpyへ変換
del output_data 
gc.collect()


output_data = pd.DataFrame(integrate(output_ndarray),columns = columns)
del output_ndarray
gc.collect()

print('output.csvを出力中....')
output_data.to_csv('../output.csv')
temp_length = len(list(output_data["Input_area"]))

elapsed_time = str(time.time() - start)
print(str(temp_length) + 'records, completed...!! it takes ' + elapsed_time + '[sec]' )

