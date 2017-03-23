# coding: utf-8

from __future__ import print_function
from multiprocessing import Pool
from multiprocessing import Process
import sys
import gc
import numpy as np
cimport numpy as np

proc = 6 

DTYPE = np.double
ctypedef np.double_t DTYPE_t


#連関構造推計用プログラム

#受注企業毎の取引品目及び取引額の組を返す

def get_index_cd_trans(data):
    cdef int i
    trans = [[[],[]] for i in xrange(len(data[3]))]
    #cdef np.ndarray[DTYPE_t,ndim=1] value
    z_cd = data[0]
    goods = data[1]
    #value = np.ndarray(data[2][:])
    value = data[2]
    z_cd_list = data[4]
    pool_number = data[5]
    for index in xrange(len(z_cd)):
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
    trans = [goods_value for i in xrange(len(z_cd_list))]
    div_z_cd = divide_lists(z_cd,proc)
    div_goods = divide_lists(goods,proc)
    div_value = divide_lists(value,proc)
    data = []
    cdef int i, j 
    for i in xrange(len(div_z_cd)):
        a = [div_z_cd[i],div_goods[i],div_value[i], trans,z_cd_list,i ]
        data.append(a)

    p = Pool(proc)
    calc = p.map(get_index_cd_trans,data)
    p.terminate()
    p.join()
    
    del data, goods_value, trans, div_z_cd, div_goods, div_value
    gc.collect()
    
    result = calc[0][:]
    for i in xrange(len(calc) - 1):
        for j in xrange(len(calc[i + 1])):
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
    goods_value = [0 for i in xrange(len(goodsSet))]
    cdef int i 
    for i in xrange(len(cd[0])):
        if pool_number == 0:
            print('now getting rate:', i, '/', len(cd[0]) , '\r', end="")
            sys.stdout.flush()
        goods_value[goodsSet.index(cd[0][i])] += cd[1][i]
    rate = [goods_value[i]/value_sum for i in xrange(len(goodsSet))]
    goodsSet_rate = [goodsSet,rate]
    
    del rate, goods_value, value_sum, goodsSet, cd
    gc.collect()
    
    return goodsSet_rate

def multi_get_rate(cd_trans):
    p = Pool(proc)
    data = []
    cdef int i 
    for i in xrange(len(cd_trans)):
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
    cdef int i, j
    for i in xrange(len(h_cd)):
        if num == 0 or 4 or 8:
            print(i, '/', len(h_cd), '\r', end="") 
            sys.stdout.flush()
        
        
        b = Z_city[i]
        c= H_city[i]
        ind = str(z_ind[i]) + '-' + str(h_ind[i])
        d = value[i]
        g = goods[i]
        if h_cd[i] not in only_h_cd:
            a = cd_trans_rate[z_cd_list.index(h_cd[i])][:]
            for j in xrange(len(a[0])):
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
    cdef int i 
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
    for i in xrange(len(div_h_cd)):
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

def make_ind_pair_goods_pair_value_list(output_data):
    """
    業種組み合わせ,投入商品,産出商品,の順で異なった場合に行が変わる
    業種が同じ総額,産出商品の組み合わせ数を記録し業種が異なった場合に総額から割合を出す
    業種ー投入商品ー産出商品が同じ限り,組み合わせ数を記録し,異なった場合に平均を出す
    """

    #cdef np.ndarray[np.string_t,ndim=1] ind_pair_data, input_ind_data, output_ind_data
    #cdef np.ndarray[np.float_t,ndim=1] value_data
    #cdef int out_line_num, same_input_count, same_input_total

    ind_pair_data = output_data['Ind_pair'][:]
    input_ind_data = output_data['Input_industory'][:]
    output_ind_data = output_data['Output_industry'][:]
    value_data = output_data['Value'][:]

    del output_data
    gc.collect()

    temp_ind_pair = ind_pair_data[0]
    temp_input_ind = input_ind_data[0]
    temp_output_ind = output_ind_data[0]
    temp_value = value_data[0]
    ind_pair_goods_pair_value_list = [[temp_ind_pair,temp_input_ind,temp_output_ind,temp_value,1]] #[ind_pair,品目,品目,額] 
    out_line_num = 0 #ind_pair_goods_pair_value_list の桁数
    same_input_count = 1
    same_input_total = 0
    cdef int num 
    for num in  xrange(len(ind_pair_data)):
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

            

            for i  in xrange(same_input_count):
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
                for i in xrange(same_input_count):
                    if same_input_total == 0:
                        ind_pair_goods_pair_value_list[out_line_num - i][3] = 1
                    else:
                        ind_pair_goods_pair_value_list[out_line_num - i][3] /= same_input_total
                            
        temp_ind_pair = ind_pair
        temp_output_ind = output_ind
        temp_input_ind = input_ind

    del temp_ind_pair, temp_input_ind, temp_output_ind, temp_value
    gc.collect()
    
    return ind_pair_goods_pair_value_list

def make_only_h_tarans_value(data): #非受注企業データ作成並列処理用関数
    only_h_trans_value = []
    OHT = data[0][:] #only_h_trans
    IPGPVL = data[1][:] # ind_pair_goods_pair_value_list 
    for i in OHT:
        temp_input_area = i[0]
        temp_output_area = i[1]
        temp_input_ind = i[2]
        temp_value = i[4]
        temp_ind_pair = i[5]

        for j in IPGPVL:
            out_line = []
            if temp_ind_pair == j[0]:
                value = temp_value * j[3] 
                temp_output_ind = j[2]
                out_line = [temp_input_area,temp_output_area,temp_input_ind,temp_output_ind,value,temp_ind_pair]
                only_h_trans_value.append(out_line)

    del OHT, IPGPVL
    gc.collect()

    return only_h_trans_value

def multi_make_only_h_trans_value(only_h_trans,ind_pair_goods_pair_value_list):
    div_only_h_trans = divide_lists(only_h_trans,proc)
    data = [[i,ind_pair_goods_pair_value_list] for i in div_only_h_trans ]
    
    p = Pool(proc)
    temp_result = p.map(make_only_h_tarans_value, data)
    p.terminate()
    p.join()

    only_h_trans_value = []
    #並列処理したものを展開
    for i in temp_result:
        only_h_trans_value.extend(i)

    del temp_result, data
    gc.collect()

    return only_h_trans_value


def integrate(output_data):
    ta = temp_Input_area = output_data["Input_area"][0]
    tb = temp_Output_area = output_data["Output_area"][0]
    tc = temp_Input_industory = output_data["Input_industory"][0]
    td = temp_Output_industory = output_data["Output_industry"][0]
    count = 0
    temp_length = len(list(output_data["Input_area"]))

    for  num  in range(temp_length):
        print('now integrating same values...',num,'/', temp_length,'\r', end='')
        sys.stdout.flush()
        if num != 0:
            tA = output_data["Input_area"][num]
            tB = output_data["Output_area"][num]
            tC = output_data["Input_industory"][num]
            tD = output_data["Output_industry"][num]
            temp_value = output_data["Value"][num]
            if ta == tA and tb == tB and tc == tC and td == tD:
                output_data.loc[count,"Value"] += temp_value
                output_data = output_data.drop(num)
            else:
                ta = tA
                tb = tB
                tc = tC 
                td = tD
                count = num
                
    del ta, tb, tc, td,num
    gc.collect

    output_data = output_data.reset_index(drop=True)
    
    return output_data

def divide_lists(lis,num): #並列処理のproc数に合わせて分割する関数
    result = []
    length = len(lis)
    sep = length // num
    div = []
    cdef int i 
    for i in xrange(num):
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



