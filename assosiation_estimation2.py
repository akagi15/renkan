# coding: utf-8
#連関構造推計用プログラム

import pandas as pd
import csv
import assosiate2
from multiprocessing import Pool
from multiprocessing import Process
import gc

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

only_h_trans = trans_city_value_goodsSet_rate[1] #受注していない発注企業

#受注していない企業を除いた取引のまとめのみを出力
print('データ型を変更中...')
columns = ['Input_area','Output_area','Input_industory','Output_industry','Value','Ind_pair']

output = trans_city_value_goodsSet_rate[0][:]

del trans_city_value_goodsSet_rate
gc.collect()

output_data = pd.DataFrame(output, columns = columns)

del output
gc.collect()

print('temp.csvを出力中....')
output_data.to_csv('../renkan_temp.csv')

#受注していない企業対策
#--業種の組み合わせ毎の品目の組み合わせ毎の取引額の平均割合を作成する
#----dataをソート
output_data = output_data.sort_values(by = ["Ind_pair","Input_industory","Output_industry"],ascending=True)
output_data = output_data.reset_index(drop=True)

temp_ind_pair = output_data['Ind_pair'][0]
temp_input_ind = output_data['Input_industory'][0]
temp_output_ind = output_data['Output_industry'][0]
temp_value = output_data['Value'][0]
ind_pair_goods_pair_value_list = [[temp_ind_pair,temp_input_ind,temp_output_ind,temp_value,1]] #[ind_pair,品目,品目,額] 
out_line_num = 0 #ind_pair_goods_pair_value_list の桁数
same_input_count = 1
same_input_total = 0


"""
業種組み合わせ,投入商品,産出商品,の順で異なった場合に行が変わる
業種が同じ総額,産出商品の組み合わせ数を記録し業種が異なった場合に総額から割合を出す
業種ー投入商品ー産出商品が同じ限り,組み合わせ数を記録し,異なった場合に平均を出す
"""

for num in  range(len(output_data['Ind_pair'])):
    ind_pair = output_data['Ind_pair'][num]
    input_ind = output_data['Input_industory'][num]
    output_ind = output_data['Output_industry'][num]
    value = output_data['Value'][num]
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
        if num == len(list(output_data['Ind_pair'])) - 1:
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
   

del temp_ind_pair, temp_input_ind, temp_output_ind, temp_value
gc.collect()

#受注していない発注企業分の取引データを作成する

only_h_trans_value = []
for i in range(len(only_h_trans)):
    temp_input_area = only_h_trans[i][0]
    temp_output_area = only_h_trans[i][1]
    temp_input_ind = only_h_trans[i][2]
    temp_value = only_h_trans[i][4]
    temp_ind_pair = only_h_trans[i][5]

    for j in range(len(ind_pair_goods_pair_value_list)):
        out_line = []
        if temp_ind_pair == ind_pair_goods_pair_value_list[j][0]:
            value = temp_value * ind_pair_goods_pair_value_list[j][3] 
            temp_output_ind = ind_pair_goods_pair_value_list[j][2]
            out_line = [temp_input_area,temp_output_area,temp_input_ind,temp_output_ind,value,temp_ind_pair]
            only_h_trans_value.append(out_line)

del only_h_trans, ind_pair_goods_pair_value_list
gc.collect()

only_h_trans_value_data = pd.DataFrame(only_h_trans_value, columns = columns)

del only_h_trans_value 
gc.collect()

output_data = output_data.append(only_h_trans_value_data)

del only_h_trans_value_data
gc.collect()

#重複した組み合わせを削除
output_data = output_data.sort_values(by = ["Input_area","Output_area","Input_industory","Output_industry"],ascending=True)
output_data = output_data.reset_index(drop=True)

ta = temp_Input_area = output_data["Input_area"][0]
tb = temp_Output_area = output_data["Output_area"][0]
tc = temp_Input_industory = output_data["Input_industory"][0]
td = temp_Output_industory = output_data["Output_industry"][0]
count = 0

for  num  in range(len(list(output_data["Input_area"]))):
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
gc.collect()


output_data = output_data.reset_index(drop=True)
print('output.csvを出力中....')
output_data.to_csv('../output.csv')
temp_length = len(list(output_data["Input_area"]))

print(str(temp_length) + 'records, completed...!!')
