# coding: utf-8

from __future__ import print_function
from multiprocessing import Pool
from multiprocessing import Process
import sys
import gc

proc = 6 

#連関構造推計用プログラム

#データの欠損を抜くためのインデックス抽出
def index_list(data):
    lis = data[0]
    index = data[1]
    a = [lis[i] for i in index]
    return a 

def multi_index_list(data_list, index): #並列処理
    data = []
    for i in data_list:
        data.append([i,index]) 

    p = Pool(proc) #最大プロセス数
    result = p.map(index_list, data)
    p.terminate()
    p.join

    del data

    gc.collect()

    return result


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
    
    results = p.map(get_rate,data)
    p.terminate()
    p.join()

    del data
    gc.collect()
        
    return results

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
        if h_cd[i] not in only_h_cd:
            a = cd_trans_rate[z_cd_list.index(h_cd[i])][:]
            b = Z_city[i]
            c= H_city[i]
            ind = [z_ind[i],h_ind[i]]
            d = value[i]
            g = goods[i]
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
            b = Z_city[i]
            c = H_city[i]
            ind = [z_ind[i],h_ind[i]]
            out_line.append(b)
            out_line.append(c)
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

    del data, a, cd_list, div_h_cd, div_z_cd, div_Z_city, div_H_city, div_value, div_goods, div_z_ind, div_h_ind
    gc.collect()

    return result


def multi_exclude_not_sell_1(trans_city_value_goodsSet_rate, z_ind, h_ind ):
    calc = trans_city_value_goodsSet_rate
    result = []
    only_h_trans = []
    for i in calc:
        result.extend(i[0])
        only_h_trans.extend(i[1])


    pair_ind_list = [[z_ind[i], h_ind[i]] for i in range(len(z_ind))]
    
    #同じ業種の組み合わせを除外する
    delete = []
    for i in range(len(pair_ind_list)):
        if pair_ind_list.index( [pair_ind_list[i][0],pair_ind_list[i][1]]) != i and  [pair_ind_list[i][0],pair_ind_list[i][1]] == pair_ind_list[i]:
            delete.append(i)
    
    delete.reverse()        
    for i in delete:
        pair_ind_list.pop(i)

    pair_ind_list_str = [str(pair_ind_list[i]) for i in range(len(pair_ind_list))]
    pair_ind_value = [[[],[]] for i in range(len(pair_ind_list))]

    #受注していない発注企業対策
    for ra in result:
        index = pair_ind_list_str.index(str(ra[5]))
        pair_ind_value[index][0].append([ra[2],ra[3]])
        pair_ind_value[index][1].append(ra[4])

    
    for raw in result:
        raw.pop(5)
 
    div_pair_ind_value = divide_lists(pair_ind_value,3)
  
    #pair_ind_value_rejected = []
    #for pair in pair_ind_value:
    #    pair_ind_value_rejected.append(set_pair_ind_value(pair))
    
    print('分割処理開始') 
    p = Pool(3) 
    pair_ind_value_rejected = p.map(set_pair_ind_value, div_pair_ind_value)
    p.terminate()
    p.join()

    pair_ind_value_rejected_last = []
    for i in pair_ind_value_rejected:
        for j in i:
            pair_ind_value_rejected_last.append(j[:])
    
    final_result =  [result,pair_ind_value_rejected_last, pair_ind_list,pair_ind_list_str,only_h_trans]
    
    del result,  pair_ind_value,pair_ind_value_rejected, pair_ind_value_rejected_last, pair_ind_list, pair_ind_list_str, only_h_trans
    gc.collect()

    return final_result
    
def set_pair_ind_value(pair_ind_value): 
    p = pair_ind_value
    num = 0
    for x in pair_ind_value:
        num += 1
        print(num, '/', len(pair_ind_value), '\r', end="") 
        sys.stdout.flush()
        delete = []
        
        for y in range(len(x[0])):
            index = x[0].index(x[0][y]) 
            if index != y:
                x[1][index] = (x[1][index] + x[1][y])/2
                delete.append(y)

        delete.reverse()

        for i in delete:
            x[0].pop(i)
            x[1].pop(i)
    
    return p 


def multi_exclude_not_sell_2(trans_exclude_not_sell1):
   
    result =    trans_exclude_not_sell1[0]
    pair_ind_value =    trans_exclude_not_sell1[1]
    pair_ind_list =    trans_exclude_not_sell1[2] 
    pair_ind_list_str = trans_exclude_not_sell1[3]
    only_h_trans = trans_exclude_not_sell1[4]
    
    
    count = 0
    for i in only_h_trans:
        count += 1
        print('非受注企業を処理中', count, len(only_h_trans), '\r', end="")
        sys.stdout.flush()
        if str(i[2]) in pair_ind_list_str:
            same_ind_pair = pair_ind_value[pair_ind_list_str.index(str(i[2]))][:]
            for j in range(len(same_ind_pair[0])):
                out_line = []
                out_line.append(i[0])
                out_line.append(i[1])
                out_line.append(same_ind_pair[0][j][0])
                out_line.append(same_ind_pair[0][j][1])
                out_line.append(same_ind_pair[1][j])
                result.append(out_line)
    
    del pair_ind_list, pair_ind_value, pair_ind_list_str, only_h_trans
    gc.collect()

    return result

def exclude_same_record(trans_exclude_not_sell):
    #重複しているレコードをまとめる
    result = trans_exclude_not_sell
    record_set = []
    out_line = []
    for res in result:
        out_line = str([res[0],res[1],res[2],res[3]])
        record_set.append(out_line)
      
    record_set = list(set(record_set))
    final_result = [[0,0,0,0,0] for i in range(len(record_set))]
    count = 0
    for res in result:
        count += 1
        print('重複コードを統合中', count, len(result), '\r', end="")
        sys.stdout.flush()
        out_line = str([res[0],res[1],res[2],res[3]])
        final_result[record_set.index(out_line)][0] = res[0]
        final_result[record_set.index(out_line)][1] = res[1]
        final_result[record_set.index(out_line)][2] = res[2]
        final_result[record_set.index(out_line)][3] = res[3]
        final_result[record_set.index(out_line)][4] += res[4]
 
    del result, record_set, out_line
    gc.collect()
        
    return final_result



def divide_lists(lis,num):
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



