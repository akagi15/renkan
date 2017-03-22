# coding: utf-8

from __future__ import print_function
from multiprocessing import Pool
from multiprocessing import Process
import sys
import gc
import numpy as np
from numba.decorators import jit 

proc = 6 

#連関構造推計用プログラム

#受注企業毎の取引品目及び取引額の組を返す

def get_index_cd_trans(data):
    trans = [[[],[]] for i in range(len(data[3]))]
    z_cd = data[0]
    goods = data[1]
    value = data[2]
    z_cd_list = data[4]
    pool_number = data[5]
    for index in range(len(z_cd)):
         #if pool_number == 1:
         #   print('now make trans list...',index,'/',len(h_cd), '\r', end="")
         #   sys.stdout.flush()
         trans[z_cd_list.index(z_cd[index])][0].append(goods[index])
         trans[z_cd_list.index(z_cd[index])][1].append(value[index])
  
    del z_cd, goods, value, z_cd_list
    gc.collect()
  
    return trans

def multi_get_index_cd_trans(z_cd, z_cd_list, value, goods):
    goods_value = [[],[]] 
    trans = [goods_value for i in range(len(z_cd_list))]
    div_z_cd = divide_lists(z_cd,proc)
    div_goods = divide_lists(goods,proc)
    div_value = divide_lists(value,proc)
    data = []
    for i in range(len(div_z_cd)):
        a = [div_z_cd[i],div_goods[i],div_value[i], trans,z_cd_list,i ]
        data.append(a)

    p = Pool(proc)
    calc = p.map(get_index_cd_trans,data)
    p.terminate()
    p.join()
    
    del data, goods_value, trans, div_z_cd, div_goods, div_value
    gc.collect()
    
    result = calc[0][:]
    for i in range(len(calc) - 1):
        for j in range(len(calc[i + 1])):
            print( 'now aggregate trans list:', i, ":", j, "/", len(calc[i + 1]),'\r', end ='')
            sys.stdout.flush()
            result[j][0].extend(calc[i + 1][j][0][:])
            result[j][1] += calc[i +1][j][1][:]
    print('インデックス作成終了.')
  
    return result


#取引品目の集合とそれぞれに対する割合を返す
def get_rate(data):
    cd = data[0]
    pool_number = data[1]
    goodsSet = list(set(cd[0]))
    value_sum = sum(cd[1])
    goods_value = [0 for i in range(len(goodsSet))]
    for i in range(len(cd[0])):
        if pool_number == 0:
            print('now getting rate:', i, '/', len(cd[0]) , '\r', end="")
            sys.stdout.flush()
        goods_value[goodsSet.index(cd[0][i])] += cd[1][i]
    rate = [goods_value[i]/value_sum for i in range(len(goodsSet))]
    goodsSet_rate = [goodsSet,rate]
    
    del rate, goods_value, value_sum, goodsSet, cd
    gc.collect()
    
    return goodsSet_rate

def multi_get_rate(cd_trans):
    p = Pool(proc)
    data = []
    for i in range(len(cd_trans)):
        data.append([cd_trans[i],i])
    
    result = p.map(get_rate,data)
    p.terminate()
    p.join()

    del data
    gc.collect()
        
    return result

#各取引毎のデータを作成
def make_data(data):
    z_cd_list = data[0]
    h_cd = data[1]
    z_cd = data[2]
    Z_city = data[3]
    H_city = data[4]
    value = data[5]
    goods = data[6]
    cd_trans_rate = data[7][:]
    num = data[8]
    z_ind = data[9]
    h_ind = data[10]
    only_h_cd = list(set(h_cd) - set(z_cd_list)) #発注企業が受注していない場合     
    trans_city_value_goodsSet_rate =[]
    only_h_trans =[]
    for i in range(len(h_cd)):
        if num == 0 or 4 or 8:
            print(i, '/', len(h_cd), '\r', end="") 
            sys.stdout.flush()
        
        
        b = Z_city[i]
        c= H_city[i]
        ind = str(z_ind[i]) +'-' + str(h_ind[i])
        d = value[i]
        g = goods[i]
        if h_cd[i] not in only_h_cd:
            a = cd_trans_rate[z_cd_list.index(h_cd[i])][:]
            for j in range(len(a[0])):
                out_line = []
                out_line.append(b)
                out_line.append(c)
                out_line.append(g)
                out_line.append(a[0][j])
                out_line.append(d*a[1][j])
                out_line.append(ind)
                trans_city_value_goodsSet_rate.append(out_line)
        else:
            out_line = []
            ind = str(z_ind[i]) + '-' + str(h_ind[i])
            out_line.append(b)
            out_line.append(c)
            out_line.append(g)
            out_line.append(0)
            out_line.append(d)
            out_line.append(ind)
            only_h_trans.append(out_line)
    
    del z_cd, h_cd, Z_city, H_city, value, goods, cd_trans_rate, z_ind, h_ind, only_h_cd
    gc.collect()

    return [trans_city_value_goodsSet_rate,only_h_trans]

def multi_make_data(h_cd, z_cd, goods, cd_trans_rate, Z_city, H_city, value, z_ind, h_ind):
    cd_list = list(set(z_cd )) 
    div_h_cd = divide_lists(h_cd,proc)
    div_z_cd = divide_lists(z_cd,proc)
    div_Z_city = divide_lists(Z_city,proc)
    div_H_city = divide_lists(H_city,proc)
    div_value = divide_lists(value,proc)
    div_goods = divide_lists(goods,proc)
    div_z_ind = divide_lists(z_ind,proc)
    div_h_ind = divide_lists(h_ind,proc)
    data = []
    for i in range(len(div_h_cd)):
        a = [cd_list, div_h_cd[i],div_z_cd[i], div_Z_city[i], div_H_city[i], div_value[i], div_goods[i], cd_trans_rate, i, div_z_ind[i], div_h_ind[i]]
        data.append(a)

    p = Pool(proc)
    result  = p.map(make_data, data)
    p.terminate()
    p.join()

    trans_city_value_goodsSet_rate = []
    only_h_trans = []
    for res in result:
        trans_city_value_goodsSet_rate.extend(res[0])
        only_h_trans.extend(res[1])
    
        
    del data, a, cd_list, div_h_cd, div_z_cd, div_Z_city, div_H_city, div_value, div_goods, div_z_ind, div_h_ind,result
    gc.collect()

    return [trans_city_value_goodsSet_rate,only_h_trans]




def divide_lists(lis,num): #並列処理のproc数に合わせて分割する関数
    result = []
    length = len(lis)
    sep = length // num
    div = []
    for i in range(num):
        if i == 0:
            div = lis[:sep +1 ]
            result.append(div)
        elif i == proc - 1:
            div = lis[sep*i + 1:]
            result.append(div)
        else:
            div = lis[sep*i + 1 : sep*(i + 1) +1]
            result.append(div)
    
    del div
    gc.collect()
        
    return result



