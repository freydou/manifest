FILES_DIR = r'E:\Python35\code\manifest\excel'
PARA_FILE = r'E:\Python35\code\manifest\parameter.xls'
DIGITAL = ['0','1','2','3','4','5','6','7','8','9']
NON_CY_CHRG_TYPE = ['IHL','IHD','TSD']
MAINLINE = ['IC1','IC2','IC5','IC6','IC7','IC8','IC9','IC10','IC11','IC12','IC15','IC16','IC17','IC18','IC19','IC20','IC21','IC22','IC23','IC25','IC27','IC28']

import os
import pandas as pd
from pandas import Series,DataFrame
import numpy as np

def matcher():
	'''
	读取参数excel文件获取各个匹配字典
	'''
	dfpol = pd.read_excel(PARA_FILE,sheetname='POL',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictpol = dfpol.to_dict(orient='dict')
	dictpol = dictpol['中文']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	dfpod = pd.read_excel(PARA_FILE,sheetname='POD',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictpod = dfpod.to_dict(orient='dict')
	dictpod = dictpod['区域']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	dfvip = pd.read_excel(PARA_FILE,sheetname='VIP',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictvip = dfvip.to_dict(orient='dict')
	dictvip = dictvip['客户']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	dfport = pd.read_excel(PARA_FILE,sheetname='PORT',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictport = dfport.to_dict(orient='dict')
	dictport = dictport['代码']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	dfarbd = pd.read_excel(PARA_FILE,sheetname='ARBD',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictarbd = dfarbd.to_dict(orient='dict')
	dictarbd20 = dictarbd['20尺']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键20尺取第一层字典的值即整个第二层字典为实际20尺费率匹配内容
	dictarbd40 = dictarbd['40尺']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键40尺取第一层字典的值即整个第二层字典为实际40尺费率匹配内容
	return dictpol,dictpod,dictvip,dictport,dictarbd20,dictarbd40

def charge_filter(charge_type,charge_amount,ib_intermodal,ob_intermodal,svvd1):
	'''
	计算CY-CY含驳费用
	'''
	if charge_type in NON_CY_CHRG_TYPE:  #若费用代码为明确的非CY-CY含驳费用则金额置为零
		return 0
	elif str(charge_type)[0] in DIGITAL: #若费用代码为数字开头则金额置为零
		return 0
	elif charge_type == 'ARB':  #若费用代码为ARB且出口联运项包含rail则金额置为零
		if 'RAIL' in str(ob_intermodal).upper():
			return 0
		elif svvd1.split('-')[0] in MAINLINE: #若费用代码为ARB且首层航次为干线则金额置为零（为漏输的铁路项）
			return 0
		else:
			return charge_amount
	elif charge_type == 'ARD':  #若费用代码为ARD且进口联运项包含rail则金额置为零
		if 'RAIL' in str(ib_intermodal).upper():
			return 0
		else:
			return charge_amount
	else:
		return charge_amount  #否则直接复制获取金额

def cntr_type_filter(ct): 
	'''
	标注标准箱型（20GP/20HQ/40GP/40HQ）
	'''
	if ct in ['20GP','20HQ']:  
		return '20尺'
	elif ct in ['40GP','40HQ']:
		return '40尺'
	else:
		return ''

def epanasia_filter(cp): 
	'''
	标注泛亚电商货
	'''
	if 'EPANASIA' in str(cp).upper():
		return 'Y'
	else:
		return 'N'

def vip_filter(cso,dict): 
	'''
	标注签约客户
	'''
	if cso[0:8] in dict: #判断CSO号前八位数字是否在签约客户的约号中
		return dict[cso[0:8]]
	elif cso in dict:   #判断CSO号是否在签约客户的约号中
		return dict[cso]
	else:
		return 'N'

def weight_filter(cn,weight):
	'''
	用于计算箱重量，剔除重复计重
	'''
	if cn == 0:
		return 0
	else:
		return weight

def arbd_route_filter(cn,ct,ts,pol,v1,ts1,v2,ts2,v3,ts3,v4,pod,fnd):
	'''
	用于筛选出arb和ard的驳船路径
	'''
	if int(ts) == 0:
		return ''
	else:
		input_port_list = [pol,pod,fnd,ts1,ts2,ts3] #所有舱单输入的涉及港口包含空 
		input_vessel_list = [v1,v2,v3,v4] #所有舱单输入的承载航次包含空
		vessel_no = int(ts)+1 #实际承载航次数为中转数加1
		port_no = int(ts)+3 #实际涉及港口数为中转数加3，即起运港，卸港，目的港加（中转港*中转次数）
		vessel_list = []
		port_list = []
		#获取实际的承载航次集合
		for i in range(0,vessel_no):
			vessel_list.append(input_vessel_list[i])
		#获取实际的涉及港口集合
		for i in range(0,port_no):
			if i > 2:  #TS港口中的代码为英文，需转换为代码以保持与POL,POD,FND的一致
				port_list.append(port_dict[input_port_list[i].strip().upper()])
			else:
				port_list.append(input_port_list[i])
		result = ''
		i = 0 #用于记录涉及港口的位置
		for vsl in vessel_list:
			if vsl.split('-')[0] == '':                   #若匹配到虚拟空航次，则代表是驳船且当前承载航次路径为最后一段路径即最后一个中转港到卸港/最终目的港
				if port_list[-1] == port_list[1]:     #若最后一中转港等于卸港，则最后一段路径取最后一卸港到最终目的港
					result = result + port_list[-1]+'-'+port_list[2]+','
				else:
					result = result + port_list[-1]+'-'+port_list[1]+','
			elif vsl.split('-')[0] in MAINLINE:           #若匹配到干线航次则当前位置数加一并继续下一次循环
				i = i + 1
				continue
			else:                                         #若未匹配到干线航次或虚拟空航次则代表当前匹配到普通驳船航次
				if i == 0:                                #代表当前处理的是第一段路径即起运港到第一中转港
					result = result +port_list[0]+'-'+port_list[3]+','
					i = i + 1 
				elif i == vessel_no - 1:                  #代表当前处理的是最后一段路径即最后一中转港到卸港/最终目的港
					if  port_list[-1] == port_list[1]:     #若最后一中转港等于卸港，则最后一段路径取最后一卸港到最终目的港
						result = result + port_list[-1]+'-'+port_list[2]+','
					else:
						result = result + port_list[-1]+'-'+port_list[1]+','
					break
				else:                                     #代表当前处理的是中间路径即中转港到中转港
					result = result + port_list[i+2]+'-'+port_list[i+3]+','
					i = i + 1
		return result
						
def arbd_route_match_filter(arbd,ct):
	'''
	用于匹配筛选出来的arb和ard驳船路径的费率
	'''
	if ct == '20尺':          #使用20尺费率字典
		arbd_dict = arbd_dict20
	elif ct == '40尺':        #使用40尺费率字典
		arbd_dict = arbd_dict40
	else:
		return 0
	amount = 0
	info = ''
	if arbd == '': #若手工ARBARD路径为空则直接返回        
		return 0#,'无驳船路径'
	else:
		routes = arbd[0:-1].split(',') #去除路径字符串末尾的分隔符，同时将多层路径（如果存在）以逗号分隔存储在列表中
		if len(routes) == 1:	 #若只有一层路径则直接匹配费率
			try:
				amount = arbd_dict[routes[0]]
				return amount#,'一层驳船路径'
			except KeyError:
				amount = 0
				#info = routes[0] + '未匹配到费率；'
				return amount#,info
		elif len(routes) > 1:
			i = 0
			while i<len(routes):
				if (i<len(routes)-1) and (routes[i].split('-')[1] == routes[i+1].split('-')[0]): #若非最后一层路径且前后层路径相连则匹配两层路径的总起运港和总卸港
					try:
						amount = amount + arbd_dict[(routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1])]
						i = i + 2 #若匹配到两层路径则下次循环跳过后程路径
						#info = info + '两层连续驳船路径；'
					except KeyError:
						amount = 0
						i = i + 2
						#info = '两层连续驳船路径'+ routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1] + '未匹配到费率；'
				else:
					try:
						amount = amount + arbd_dict[routes[i]]
						i = i + 1
					except KeyError:
						amount = 0
						i = i + 1
						#info = routes[i] + '未匹配到费率；'
			return amount#,info

def arbd_route_info_filter(arbd,ct):
	'''
	用于是否匹配到arb和ard驳船路径的信息
	'''
	if ct == '20尺':
		arbd_dict = arbd_dict20
	elif ct == '40尺':
		arbd_dict = arbd_dict40
	else:
		return '非标准箱型'
	info = ''
	if arbd == '': #若手工ARBARD路径为空则直接返回        
		return '无驳船路径'
	else:
		routes = arbd[0:-1].split(',') #去除路径字符串末尾的分隔符，同时将多层路径（如果存在）以逗号分隔存储在列表中
		if len(routes) == 1:	 #若只有一层路径则直接匹配费率
			try:
				arbd_dict[routes[0]]
				return '一层驳船路径'
			except KeyError:
				info = routes[0] + '未匹配到费率；'
				return info
		elif len(routes) > 1:
			i = 0
			while i<len(routes):
				if (i<len(routes)-1) and (routes[i].split('-')[1] == routes[i+1].split('-')[0]): #若非最后一层路径且前后层路径相连则匹配两层路径的总起运港和总卸港
					try:
						arbd_dict[(routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1])]
						i = i + 2 #若匹配到两层路径则下次循环跳过后程路径
						info = info + '两层连续驳船路径；'
					except KeyError:
						info = '两层连续驳船路径'+ routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1] + '未匹配到费率；'
						i = i + 2
				else:
					try:
						arbd_dict[routes[i]]
						i = i + 1
					except KeyError:
						info = routes[i] + '未匹配到费率；'
						i = i + 1
			return info

def lump_sum_tms(ct,bl,svvd1,svvd2,svvd3,svvd4,tms):
	'''
	对箱型为空的记录进行terms匹配
	'''
	if ct == '空':   #将按提单收取的费用的空箱型的terms置为和其提单号及各SVVD完全一致的记录的非空箱型的terms以便合计时合并
		matchdf = df[df['BL REF CDE']== bl]
		matchdf = matchdf[matchdf['SVVD1']==svvd1] 
		matchdf = matchdf[matchdf['SVVD2']==svvd2]
		matchdf = matchdf[matchdf['SVVD3']==svvd3]
		matchdf = matchdf[matchdf['SVVD4']==svvd4]
		matchdf = matchdf[matchdf['CNTR TYPE'] != '空'] 
		matchtms = matchdf['TERMS'].unique()[0] #取单一值 
		return matchtms
	else:
		return tms

def lump_sum_comm(ct,bl,svvd1,svvd2,svvd3,svvd4,comm):
	'''
	对箱型为空的记录进行comm匹配
	'''
	if ct == '空':   #将按提单收取的费用的空箱型的comm置为和其提单号及各SVVD完全一致的记录的非空箱型的comm以便合计时合并
		matchdf = df[df['BL REF CDE']== bl]
		matchdf = matchdf[matchdf['SVVD1']==svvd1] 
		matchdf = matchdf[matchdf['SVVD2']==svvd2]
		matchdf = matchdf[matchdf['SVVD3']==svvd3]
		matchdf = matchdf[matchdf['SVVD4']==svvd4]
		matchdf = matchdf[matchdf['CNTR TYPE'] != '空'] 
		matchcomm = matchdf['COMM'].unique()[0] #取单一值 
		return matchcomm
	else:
		return comm

def lump_sum_bd(ct,bl,svvd1,svvd2,svvd3,svvd4,bd):
	'''
	对箱型为空的记录进行BRIEF DESC匹配
	'''
	if ct == '空':   #将按提单收取的费用的空箱型的brief desc置为和其提单号及各SVVD完全一致的记录的非空箱型的brief desc以便合计时合并
		matchdf = df[df['BL REF CDE']== bl]
		matchdf = matchdf[matchdf['SVVD1']==svvd1] 
		matchdf = matchdf[matchdf['SVVD2']==svvd2]
		matchdf = matchdf[matchdf['SVVD3']==svvd3]
		matchdf = matchdf[matchdf['SVVD4']==svvd4]
		matchdf = matchdf[matchdf['CNTR TYPE'] != '空'] 
		matchbd = matchdf['BRIEF DESC'].unique()[0] #取单一值 
		return matchbd
	else:
		return bd

def lump_sum_soc(ct,bl,svvd1,svvd2,svvd3,svvd4,soc):
	'''
	对箱型为空的记录进行SOC匹配
	'''
	if ct == '空':   #将按提单收取的费用的空箱型的SOC置为和其提单号及各SVVD完全一致的记录的非空箱型的SOC以便合计时合并
		matchdf = df[df['BL REF CDE']== bl]
		matchdf = matchdf[matchdf['SVVD1']==svvd1] 
		matchdf = matchdf[matchdf['SVVD2']==svvd2]
		matchdf = matchdf[matchdf['SVVD3']==svvd3]
		matchdf = matchdf[matchdf['SVVD4']==svvd4]
		matchdf = matchdf[matchdf['CNTR TYPE'] != '空'] 
		matchsoc = matchdf['SOC'].unique()[0] #取单一值 
		return matchsoc
	else:
		return soc

def lump_sum_cno(ct,bl,svvd1,svvd2,svvd3,svvd4,cno):
	'''
	对箱型为空的记录进行箱号匹配
	'''
	if ct == '空':   #将按提单收取的费用的空箱型的箱号置为和其提单号及各SVVD完全一致的记录的非空箱型的箱号以便合计时合并
		matchdf = df[df['BL REF CDE']== bl]
		matchdf = matchdf[matchdf['SVVD1']==svvd1] 
		matchdf = matchdf[matchdf['SVVD2']==svvd2]
		matchdf = matchdf[matchdf['SVVD3']==svvd3]
		matchdf = matchdf[matchdf['SVVD4']==svvd4]
		matchdf = matchdf[matchdf['CNTR TYPE'] != '空'] 
		matchcno = matchdf['CNTR NUM'].unique()[0] #取单一值 
		return matchcno
	else:
		return cno
	
def lump_sum_ct(ct,bl,svvd1,svvd2,svvd3,svvd4):
	'''
	对箱型为空的记录进行箱型匹配,注意这个需在前几个匹配后进行
	'''
	if ct == '空':   #将按提单收取的费用的空箱型置为和其提单号及各SVVD完全一致的记录的非空箱型以便合计时合并
		matchdf = df[df['BL REF CDE']== bl]
		matchdf = matchdf[matchdf['SVVD1']==svvd1] 
		matchdf = matchdf[matchdf['SVVD2']==svvd2]
		matchdf = matchdf[matchdf['SVVD3']==svvd3]
		matchdf = matchdf[matchdf['SVVD4']==svvd4]
		matchdf = matchdf[matchdf['CNTR TYPE'] != '空'] 
		matchct = matchdf['CNTR TYPE'].unique()[0] #取单一值 
		return matchct
	else:
		return ct
		
filenames = os.listdir(FILES_DIR) #遍历文件夹获取所有文件的文件名

pol_dict,pod_dict,vip_dict,port_dict,arbd_dict20,arbd_dict40= matcher() #获取匹配字典

for filename in filenames:
	fn = filename.split('.')[0] #获取文件名即航次名
	full_filename = FILES_DIR + '/' + filename
	#df = pd.read_excel(full_filename)
	df = pd.read_excel(full_filename,skiprows = 3) #跳过前三行信息区域
	df = df[df['BL REF CDE'] != ' '] #将空行删除
	df = df[df['BL REF CDE'] != 'TOTAL'] #将汇总行删除
	
	#--以下增加各新列--#
	df['船名航次'] = fn #取文件名为船名航次
	df['SVVD1'] = df['SVVD1'].fillna('')#对空值赋值
	df['CY-CY含驳费用'] = df.apply(lambda x:charge_filter(charge_type=x['CHRG TYPE'],charge_amount=x['TTL AMT'],ib_intermodal=x['I/B Intermodal '],ob_intermodal=x['O/B Intermodal'],svvd1=x['SVVD1']), axis=1)#获取CY-CY含驳费用
	df['电商'] = df.apply(lambda x:epanasia_filter(cp=x['CONTROL PARTY']), axis=1) #匹配电商货
	df['CN12'] = df['CN12'].fillna(0) #对空值赋值
	df['CN20'] = df['CN20'].fillna(0) #对空值赋值
	df['CN40'] = df['CN40'].fillna(0) #对空值赋值
	df['CN45'] = df['CN45'].fillna(0) #对空值赋值
	df['箱量'] = df['CN12'] + df['CN20'] + df['CN40'] + df['CN45'] #获取用于透视表求和统计的箱量，也可用于删除同一对象的重复数量属性
	df['POL CDE'] = df['POL CDE'].fillna('XXX') #对空值赋值
	df['POD CDE'] = df['POD CDE'].fillna('XXX') #对空值赋值
	df['FND CDE'] = df['FND CDE'].fillna('XXX') #对空值赋值
	df['起运港'] = [pol_dict[x] for x in df['POL CDE']] #通过匹配字典将起运港代码转换为中文起运港
	df['卸港区域'] = [pod_dict[x] for x in df['POD CDE']] #通过匹配字典将卸港代码转换为中文卸港区域
	df['CSO NO'] = df['CSO NO'].fillna('空白') #对空值赋值
	df['签约客户'] = df.apply(lambda x:vip_filter(cso=x['CSO NO'],dict=vip_dict),axis=1)#匹配签约客户
	df['WEIGHT'] = df['WEIGHT'].fillna(0) #对空值赋值
	df['重量'] = df.apply(lambda x:weight_filter(cn=x['箱量'],weight=x['WEIGHT']),axis=1)#获取用于透视表求和统计的重量
	
	#--以下对箱型为空的按提单收取费用记录进行处理，注意维度列需复制非空的对应数据以便合并而数值类不用复制以避免重复计算--#
	df['SVVD2'] = df['SVVD2'].fillna('')#对空值赋值
	df['SVVD3'] = df['SVVD3'].fillna('')#对空值赋值
	df['SVVD4'] = df['SVVD4'].fillna('')#对空值赋值
	df['TERMS'] = df['TERMS'].fillna('')#对空值赋值
	df['COMM'] = df['COMM'].fillna('')#对空值赋值
	df['SOC'] = df['SOC'].fillna('') #对空值赋值
	df['BRIEF DESC'] = df['BRIEF DESC'].fillna('')#对空值赋值
	df['CNTR NUM'] = df['CNTR NUM'].fillna('')#对空值赋值
	df['CNTR TYPE'] = df['CNTR TYPE'].fillna('空') #对空值赋值
	df['SOC'] = df.apply(lambda x:lump_sum_soc(ct=x['CNTR TYPE'],bl=x['BL REF CDE'],svvd1=x['SVVD1'],svvd2=x['SVVD2'],svvd3=x['SVVD3'],svvd4=x['SVVD4'],soc=x['SOC']), axis=1)
	df['BRIEF DESC'] = df.apply(lambda x:lump_sum_bd(ct=x['CNTR TYPE'],bl=x['BL REF CDE'],svvd1=x['SVVD1'],svvd2=x['SVVD2'],svvd3=x['SVVD3'],svvd4=x['SVVD4'],bd=x['BRIEF DESC']), axis=1)
	df['COMM'] = df.apply(lambda x:lump_sum_comm(ct=x['CNTR TYPE'],bl=x['BL REF CDE'],svvd1=x['SVVD1'],svvd2=x['SVVD2'],svvd3=x['SVVD3'],svvd4=x['SVVD4'],comm=x['COMM']), axis=1)
	df['TERMS'] = df.apply(lambda x:lump_sum_tms(ct=x['CNTR TYPE'],bl=x['BL REF CDE'],svvd1=x['SVVD1'],svvd2=x['SVVD2'],svvd3=x['SVVD3'],svvd4=x['SVVD4'],tms=x['TERMS']), axis=1)
	#df['CNTR NUM'] = df.apply(lambda x:lump_sum_cno(ct=x['CNTR TYPE'],bl=x['BL REF CDE'],svvd1=x['SVVD1'],svvd2=x['SVVD2'],svvd3=x['SVVD3'],svvd4=x['SVVD4'],cno=x['CNTR NUM']), axis=1)
	#注意以下对空箱型的箱型列赋值必须放在各列赋值的最后，因为以上赋值都运用到了空箱型的空属性
	df['CNTR TYPE'] = df.apply(lambda x:lump_sum_ct(ct=x['CNTR TYPE'],bl=x['BL REF CDE'],svvd1=x['SVVD1'],svvd2=x['SVVD2'],svvd3=x['SVVD3'],svvd4=x['SVVD4']), axis=1)
	
	#--以下进行手工ARB\ARD的处理--#
	df['箱型'] = df.apply(lambda x:cntr_type_filter(ct=x['CNTR TYPE']), axis=1)
	df['TS'] = df['TS'].fillna(0) #对空值赋值
	df['TS1'] = df['TS1'].fillna('')#对空值赋值
	df['TS2'] = df['TS2'].fillna('')#对空值赋值
	df['TS3'] = df['TS3'].fillna('')#对空值赋值
	df['手工ARBARD路径'] = df.apply(lambda x:arbd_route_filter(cn=x['箱量'],ct=x['CNTR TYPE'],ts=x['TS'],pol=x['POL CDE'],v1=x['SVVD1'],ts1=x['TS1'],v2=x['SVVD2'],ts2=x['TS2'],v3=x['SVVD3'],ts3=x['TS3'],v4=x['SVVD4'],pod=x['POD CDE'],fnd=x['FND CDE']),axis=1)
	df['手工ARBARD费用'] = df.apply(lambda x:arbd_route_match_filter(arbd=x['手工ARBARD路径'],ct=x['箱型']),axis=1)
	df['手工ARBARD信息'] = df.apply(lambda x:arbd_route_info_filter(arbd=x['手工ARBARD路径'],ct=x['箱型']),axis=1)
	
	#--以下输出结果--#
	result = pd.pivot_table(df,values=['箱量','CY-CY含驳费用','重量'],index=['船名航次','BL REF CDE','CNTR NUM','POL','POD','FND','起运港','卸港区域','电商','签约客户','箱型','CNTR TYPE','CSO NO','SOC','CONTROL PARTY','SHIPPER','Booking Party','COMM','BRIEF DESC','TERMS','手工ARBARD路径','手工ARBARD费用','手工ARBARD信息'],aggfunc=np.sum)
	writer = pd.ExcelWriter(fn+'（处理后）.xls')
	#df.to_excel(writer,'result',merge_cells=False,index=False)
	result.to_excel(writer,'result',merge_cells=False,index=True)
	writer.save()
