FILES_DIR = 'E:/Python33/code/manifest/manifestexcel'
PARA_FILE = 'E:/Python33/code/manifest/parameter.xls'
OUTPUT_ADDR = 'E:/Python33/code/manifest'

DIGITAL = ['0','1','2','3','4','5','6','7','8','9']
NON_CY_CHRG_TYPE = ['IHL','IHD','TSD','PSU','DPS','BKF','IBS']
OCEAN_ARBD_CHRG_TYPE = ['Ocean Freight','EBS','BAF','BUC','ARB','ARD']
COMMISSION_RELATED_CHAR_TYPE = ['ARB','ARD','BAF','CRM','EBS','FFC','IDO','IHD','IHL','Ocean Freight','SLF','THC','THD']
INCLUDE_RELATED_CHAR_TYPE = ['ARB','ARD','BAF','CRM','EBS','FFC','SLF','THC','THD']

LOCAL_CHRG_TYPE = ['PSU','DPS','BKF','IBS']
DDS_CHRG_TYPE = ['DOC','DCI','SLF']
EPANASIA_CHRG_TYPE = ['7EC','7EI','7EO','7EN']
MAINLINE = ['IC0','IC1','IC2','IC4','IC5','IC6','IC7','IC8','IC9','IC10','IC11','IC12','IC15','IC16','IC17','IC18','IC19','IC20','IC21','IC22','IC23','IC25','IC26','IC27','IC28','CF3'] #干线航次集合，注意IC0代表虚拟空干线航次
BASEPORT_ROUTE_EXCEPTION = ['DCB-DOJ','DOJ-DCB','NSH-DOJ','DOJ-NSH','GLA-DOJ','NSH-HUM','HUM-NSH','GLA-HUM','DCB-HUM','HUI-NSH','NSH-HUI','HUI-DCB','DCB-HUI','HUI-GLA','GLA-HUI','NSH-GLA','NSH-DCB','DCB-NSH','FOC-FZN','FZN-FOC','XMN-ZZU','TSN-CFD','DLC-HHA','APU-NGB','NGB-APU','TAO-LYG','WUC-NSH','NSH-WUC'] #路径为基本港之间非干线（先后顺序重要）,涉及虎门东江仓惠州乌冲口南沙大铲湾高栏、天津曹妃甸、乍浦宁波、青岛连云港
MAINLINE_DEFINED_REGION_RULE = {'海南华南福建福建':1,'西南华南福建福建':1,'海南华南西南':0}  #手工指定显性的区域级别的主干线判断规则，用于处理区域级别普通主干线判断规则（跨不同区域优先级高）无法处理的特殊情况，以{区域序列：干线集合中主干线的顺序号（从0开始计数）}为键值对的字典存储

import os
import pandas as pd
from pandas import Series,DataFrame
import numpy as np
import time

def matcher():
	'''
	读取参数excel文件获取各个匹配字典
	'''
	dfpol = pd.read_excel(PARA_FILE,sheetname='S_POL',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictpol = dfpol.to_dict(orient='dict')
	dictpol = dictpol['中文']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	dfpod = pd.read_excel(PARA_FILE,sheetname='S_POD',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictpod = dfpod.to_dict(orient='dict')
	dictpod = dictpod['区域']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	n_dfpol = pd.read_excel(PARA_FILE,sheetname='N_POL',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	n_dictpol = n_dfpol.to_dict(orient='dict')
	n_dictpol = n_dictpol['区域']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	n_dfpod = pd.read_excel(PARA_FILE,sheetname='N_POD',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	n_dictpod = n_dfpod.to_dict(orient='dict')
	n_dictpod = n_dictpod['中文']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
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
	dfbaseport = pd.read_excel(PARA_FILE,sheetname='BASEPORT',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictbaseport = dfbaseport.to_dict(orient='dict')
	dictbaseportchinesename = dictbaseport['中文名']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容，注意实际使用该字典用的是键而非值，影响参数表的先后顺序
	dictbaseportregion = dictbaseport['区域'] #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	dictbaseporthighregion = dictbaseport['大区域'] #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	dfscts = pd.read_excel(PARA_FILE,sheetname='SCTSPORT',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictscts = dfscts.to_dict(orient='dict')
	dict_sc_ts_nameclass = dictscts['类别']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际中转点名字和类别对匹配内容
	dfcjts = pd.read_excel(PARA_FILE,sheetname='CJTSPORT',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictcjts = dfcjts.to_dict(orient='dict')
	dict_cj_ts_nameclass = dictcjts['类别']  #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际中转点名字和类别对匹配内容
	dfarbdreplace = pd.read_excel(PARA_FILE,sheetname='ARBDREPLACE',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictarbd_replace = dfarbdreplace.to_dict(orient='dict')
	dictarbd_replace_route = dictarbd_replace['替换路径'] #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容

	dfcargoflow = pd.read_excel(PARA_FILE,sheetname='DIRECTION',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictcargoflow = dfcargoflow.to_dict(orient='dict')
	dictdirection = dictcargoflow['流向'] #由于to_dict函数默认有二层字典，因此通过第一层字典的键取第一层字典的值即整个第二层字典为实际匹配内容
	return dictpol,dictpod,n_dictpol,n_dictpod,dictvip,dictport,dictarbd20,dictarbd40,dictbaseportchinesename,dictbaseportregion,dictbaseporthighregion,dict_sc_ts_nameclass,dict_cj_ts_nameclass,dictarbd_replace_route,dictdirection
def charge_filter(charge_type,charge_amount,ib_intermodal,ob_intermodal,svvd1,por):
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
		elif svvd1.split('-')[0] in MAINLINE: #若费用代码为ARB且首层航次为干线且por不为北海、铁山、防城则金额置为零（为漏输的铁路项）
			if por in ['BHY','TIE','FAN']:  #由于系统会不输北海、铁山、防城至钦州的驳船段因此该类路径实际有驳船路径但无法通过svvd1看出
				return charge_amount
			else:
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

def ihld_filter(charge_type,charge_amount):
	'''
	计算IHL、IHD费用
	'''
	if charge_type in ['IHL','IHD']:  #若费用代码为IHL或IHD则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def commission_related_charge_filter(charge_type,charge_amount):
	'''
	计算计佣费用代码
	'''
	if charge_type in COMMISSION_RELATED_CHAR_TYPE:  #若费用代码为计佣费用代码则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def include_related_charge_filter(charge_type,charge_amount,include):
	'''
	计算有Y标识的佣金舱单总收入
	'''
	#只有include项为Y且费用代码在指定类别中的才给予计算，否则返回0
	if include == '':
		return 0
	elif include == 'Y':
		if charge_type in INCLUDE_RELATED_CHAR_TYPE:
			return charge_amount
		else:
			return 0
	else:
		return 0
		
def rail_arb_filter(charge_type,charge_amount,ob_intermodal,svvd1,por):
	'''
	计算铁路ARB费用
	'''
	if charge_type == 'ARB':  #若费用代码为ARB且出口联运项包含rail则获取金额
		if 'RAIL' in str(ob_intermodal).upper():
			return charge_amount
		elif svvd1.split('-')[0] in MAINLINE: #若费用代码为ARB且首层航次为干线则除非POR为北海、铁山、防城（北海、铁山、防城到钦州驳船段系统不输）其他均为漏输RAIL的铁路段
			if por in ['BHY','TIE','FAN']:
				return 0
			else:
				return charge_amount
		else:	
			return 0
	else:
		return 0 

def rail_ard_filter(charge_type,charge_amount,ib_intermodal):
	'''
	计算铁路ARD费用
	'''
	if charge_type == 'ARD':  #若费用代码为ARD且进口联运项包含rail则获取金额
		if 'RAIL' in str(ib_intermodal).upper():
			return charge_amount
		else:
			return 0
	else:
		return 0
		
def ocean_arbd_filter(charge_type,charge_amount):
	'''
	计算纯海含驳(铁)费用
	'''
	if charge_type in OCEAN_ARBD_CHRG_TYPE:  #若费用代码为纯海含驳费用则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def local_charge_filter(charge_type,charge_amount):
	'''
	计算local charge费用
	'''
	if charge_type in LOCAL_CHRG_TYPE:  #若费用代码为local charge费用则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def psu_filter(charge_type,charge_amount):
	'''
	计算PSU费用
	'''
	if charge_type == 'PSU':  #若费用代码为PSU则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0
		
def dps_filter(charge_type,charge_amount):
	'''
	计算DPS费用
	'''
	if charge_type == 'DPS':  #若费用代码为DPS则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def bkf_filter(charge_type,charge_amount):
	'''
	计算BKF费用
	'''
	if charge_type == 'BKF':  #若费用代码为BKF则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def ibs_filter(charge_type,charge_amount):
	'''
	计算IBS费用
	'''
	if charge_type == 'IBS':  #若费用代码为IBS则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0
		
def ocb_filter(charge_type,charge_amount):
	'''
	计算OCB费用
	'''
	if charge_type == 'Ocean Freight':  #若费用代码为OCB则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def baf_filter(charge_type,charge_amount):
	'''
	计算BAF费用
	'''
	if charge_type == 'BAF':  #若费用代码为BAF则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def ebs_filter(charge_type,charge_amount):
	'''
	计算EBS费用
	'''
	if charge_type == 'EBS':  #若费用代码为EBS则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def buc_filter(charge_type,charge_amount):
	'''
	计算BUC费用
	'''
	if charge_type == 'BUC':  #若费用代码为BUC则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0
		
def dcd_filter(charge_type,charge_amount):
	'''
	计算DCD费用
	'''
	if charge_type == 'DCD':  #若费用代码为DCD则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0
		
def hsd_filter(charge_type,charge_amount):
	'''
	计算HSD费用
	'''
	if charge_type == 'HSD':  #若费用代码为HSD则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def icd_filter(charge_type,charge_amount):
	'''
	计算ICD费用
	'''
	if charge_type == 'ICD':  #若费用代码为ICD则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def tsd_filter(charge_type,charge_amount):
	'''
	计算TSD费用
	'''
	if charge_type == 'TSD':  #若费用代码为TSD则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def epanasia_charge_filter(charge_type,charge_amount):
	'''
	计算电商CODE
	'''
	if charge_type in EPANASIA_CHRG_TYPE:  #若费用代码为电商code则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def dds_charge_filter(charge_type,charge_amount):
	'''
	计算DDS CODE
	'''
	if charge_type in DDS_CHRG_TYPE:  #若费用代码为DDS code则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0		
		
def _7ec_filter(charge_type,charge_amount):
	'''
	计算7EC费用
	'''
	if charge_type == '7EC':  #若费用代码为7EC则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0		

def _7ei_filter(charge_type,charge_amount):
	'''
	计算7EI费用
	'''
	if charge_type == '7EI':  #若费用代码为7EI则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def _7eo_filter(charge_type,charge_amount):
	'''
	计算7EO费用
	'''
	if charge_type == '7EO':  #若费用代码为7EO则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0

def _7en_filter(charge_type,charge_amount):
	'''
	计算7EN费用
	'''
	if charge_type == '7EN':  #若费用代码为7EN则获取金额
		return charge_amount
	else:
		return 0  #否则直接置为0
	
def cntr_type_filter(ct): 
	'''
	标注标准箱型（20GP/20HQ/40GP/40HQ）
	'''
	if ct in ['20GP','20HQ']:  
		return '20尺标'
	elif ct in ['40GP','40HQ']:
		return '40尺标'
	elif ct.startswith('2'):
		return '20尺非标'
	elif ct.startswith('40'):
		return '40尺非标'
	elif ct.startswith('45'):
		return '45尺'

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
	if cso[0:8] in dict: #判断CSO号前八位字符是否在签约客户的约号中
		return dict[cso[0:8]]
	elif cso[0:11] in dict: #判断CSO号前十一位字符是否在签约客户的约号中
		return dict[cso[0:11]]
	elif cso.upper() in dict:   #判断完整CSO号是否在签约客户的约号中
		return dict[cso.upper()]
	else:
		return 'N'
		
def direction_matcher(polpod,dict):
	'''
	根据起卸对信息判断流向
	'''
	if polpod in dict:
		return dict[polpod]
	else:
		return '未匹配'
		
def weight_filter(cn,weight):
	'''
	用于计算箱重量，剔除重复计重
	'''
	if cn == 0:
		return 0
	else:
		return weight


def ts_port_correcter(pre_vsl,tsport):
	'''
	将TS中错输的中转港（驳点或基港）进行替换
	'''
	global errmsg
	if tsport.strip().lower() == '': #若TS为空，则直接返回
		return ''
	elif tsport.strip().lower() == 'maoming':   #若TS中错输的驳点为茂名，则用湛江代替
		ts = 'zhanjiang'
	elif tsport.strip().lower() == 'ningde':   #若TS中错输的驳点为宁德，则用福州代替
		ts = 'fuzhou new port'	
	elif tsport.strip().lower() in ['tieshangang','tieshan','beihai','fangcheng']:  #若TS中错输的驳点为铁山北航防城，则用钦州代替
		ts = 'qinzhou'
	elif tsport.strip().lower() == 'huanghua': #若TS中输的是huanghua且前置航线为IC8则由大连代替
		if pre_vsl.split('-')[0] == 'IC8':
			ts = 'dalian'
		elif pre_vsl.split('-')[0] == 'IC26': #若TS中输的是huanghua且前置航线为IC26则由天津代替
			ts = 'xingang'
		else:
			ts = 'huanghua'
	elif tsport.strip().lower() in ['dandong','dongying','weihai','weifang','longkou']: #若TS中输的是渤海湾驳点且前置航线为IC8或IC15则由大连代替
		if pre_vsl.split('-')[0] in ['IC8','IC15']:
			ts = 'dalian'
		else:
			ts = tsport.strip().lower()
	elif tsport.strip().lower() == 'shantou': #若TS中输的是shantou且前置航线为IC7或IC19则由厦门代替
		if pre_vsl.split('-')[0] in ['IC7','IC19']:
			ts = 'xiamen'
		else:
			ts = 'shantou'
	elif tsport.strip().lower() == 'quanzhou': #若TS中输的是quanzhou且前置航线为IC7或IC19则由厦门代替
		if pre_vsl.split('-')[0] in ['IC7','IC19']:
			ts = 'xiamen'
		else:
			ts = 'quanzhou'
	elif tsport.strip().lower() == 'qinzhou': #若TS中输的是qinzhou且前置航线为IC17则由湛江代替
		if pre_vsl.split('-')[0] == 'IC17':
			ts = 'zhanjiang'
		else:
			ts = 'qinzhou'
	elif tsport.strip().lower() == 'zhapu': #若TS中输的是zhapu且前置航线为IC9或IC15或IC22则由宁波代替
		if pre_vsl.split('-')[0] in ['IC15','IC22','IC9']:
			ts = 'ningbo'
		elif pre_vsl.split('-')[0] == 'IC7': #若TS中输的是zhapu且前置航线为IC7则由上海代替
			ts = 'shanghai'
		else:
			ts = 'zhapu'
	elif tsport.strip().lower() == 'dongjiang': #若TS中输的是dongjiang且前置航线为IC16则由高栏代替
		if pre_vsl.split('-')[0] == 'IC16':
			ts = 'gaolan'
		elif pre_vsl.split('-')[0] == 'IC11':  #若TS中输的是dongjiang且前置航线为IC11则由大铲湾代替
			ts = 'dachan bay'
		else:
			ts = 'dongjiang'
	elif tsport.strip().lower() == 'wenzhou': #若TS中输的是wenzhou且前置航线为IC15则由宁波代替
		if pre_vsl.split('-')[0] == 'IC15':
			ts = 'ningbo'
		else:
			ts = 'wenzhou'
	elif tsport.strip().lower() == 'zhanjiang': #若TS中输的是zhanjiang且前置航线为IC16则由高栏代替
		if pre_vsl.split('-')[0] == 'IC16':
			ts = 'gaolan'
		else:
			ts = 'zhanjiang'
	elif tsport.strip().lower() == 'yangpugang': #若TS中输的是yangpugang且前置航线为IC16则由高栏代替
		if pre_vsl.split('-')[0] == 'IC16':
			ts = 'gaolan'
		else:
			ts = 'yangpugang'
	elif tsport.strip().lower() == 'humen': #若TS中输的是humen且前置航线为IC9则由南沙代替
		if pre_vsl.split('-')[0] == 'IC9':
			ts = 'nansha'
		else:
			ts = 'humen'
	elif tsport.strip().lower() in cj_ts_nameclass_dict.keys(): #若TS中错输的是长江驳点
		if pre_vsl.split('-')[0] in ['IC16','IC26','IC17','IC20']: #若前置航线为IC16、IC26、IC17、IC20则由太仓代替
			ts = 'taicang'
		elif pre_vsl.split('-')[0] in ['IC8','IC6','IC7','IC5']:  #若前置航线为IC6或IC8或IC7或IC5则由上海代替
			ts = 'shanghai'
		else:
			ts = tsport.strip().lower()
	elif tsport.strip().lower() in sc_ts_nameclass_dict.keys():   #若TS中错输的驳点为华南驳点
		if pre_vsl.split('-')[0] in ['IC9','IC10','IC12','IC15','IC22','IC23','IC25','IC28']:  #若由左边指定航线带到华南驳点，则用南沙代替
			ts = 'nansha'
		elif pre_vsl.split('-')[0] == 'IC6' and pre_vsl.split('-')[2][-1] == 'N': #若由左边指定航线带到华南驳点且航向为N，则用高栏代替
			ts = 'gaolan'
		elif pre_vsl.split('-')[0] in ['IC11','IC16']:  #若由左边指定航线带到华南驳点，则区分分珠江驳点A和珠江驳点B用大铲湾或高栏代替
			if sc_ts_nameclass_dict[tsport.strip().lower()] == 'A':
				ts = 'dachan bay'
			elif sc_ts_nameclass_dict[tsport.strip().lower()] == 'B':
				ts = 'gaolan'
		else:
			errmsg = errmsg + pre_vsl.split('-')[0] + '后接驳点的中转港不确定' + '\n'
			ts = tsport.strip().lower()
	else:
		ts = tsport.strip().lower()
	#针对驳船基港情况进行二次核查
	if pre_vsl.split('-')[0].startswith('CB'):  #若长江支线(CBxx)出现驳船基港问题即长江支线船舶直接接非华东基本港
		if tsport.strip().lower() == 'gaolan':  #若输错的是高栏港则后面疑似IC16用太仓港代替
			ts = 'taicang'
		elif port_dict[tsport.strip().upper()] in baseport_region_dict.keys() and baseport_region_dict[port_dict[tsport.strip().upper()]] != '华东': #若输错的是其他港则用上海代替
			ts = 'shanghai'
		else:
			ts = tsport.strip().lower()
	elif (pre_vsl.split('-')[0].startswith('IC5') or pre_vsl.split('-')[0].startswith('IC6')) and (len(pre_vsl.split('-')[0]) == 4):  #若珠江支线(IC50-IC69)出现驳船基港问题即珠江支线船舶直接接非华南西南基本港
		if (port_dict[tsport.strip().upper()] in baseport_region_dict.keys()) and (baseport_region_dict[port_dict[tsport.strip().upper()]] not in ['华南','西南']):
			ts = 'gaolan'
		else:
			ts = tsport.strip().lower()

	return ts

def pod_cde_correcter(pod_cde,ts1,ts1e,ts2,ts2e,ts3,ts3e):
	'''
	若进行过TS驳点替换，则同时还需进行POD的替换
	'''
	global errmsg
	try:
		#从后往前（次序重要）依次判断时候进行过TS替换，如有则需进行POD替换,注意pod不能跨ts替换（即进行过ts替换但后面还有ts则不替换pod）
		if ts3.lower() != ts3e.lower():
			return port_dict[ts3.upper()]
		elif ts2.lower() != ts2e.lower():
			#防止pod跨ts替换
			if ts3 != '':
				return port_dict[ts2.upper()]
			else:
				return pod_cde
		elif ts1.lower() != ts1e.lower():
			#防止pod跨ts替换
			if ts3 != '' and ts2 != '':
				return port_dict[ts1.upper()]
			else:
				return pod_cde
		else:
			return pod_cde
	except KeyError:
		errmsg = errmsg + ts1.lower()+'或'+ts2.lower()+'或'+ts3.lower()+'在参数表港口字典中不存在！' + '\n'
		return pod_cde #若匹配到未定义港口则保持原状并输出异常
		
def empty_svvd_filter(cn,ct,ts,pol,v1,ts1,v2,ts2,v3,ts3,v4,pod,fnd):
	'''
	用于对svvd为空的航次进行判断干支线
	'''
	if int(ts) == 0:
		return ''
	else:
		input_port_list = [pol,pod,fnd,ts1,ts2,ts3] #所有舱单输入的涉及港口包含空 
		input_vessel_list = [v1,v2,v3,v4] #所有舱单输入的承载航次包含空
		vessel_no = int(ts)+1 #实际承载航次数为中转数加1
		port_no = int(ts)+3 #实际涉及港口数为中转数加3，即起运港，卸港，目的港加（中转港*中转次数）
		vessel_list = [] #用于存储实际承运航次
		port_list = [] #用于存储实际挂港
		result = ''  #用于存储路径提示信息
		#获取实际的承载航次集合
		for i in range(0,vessel_no):
			vessel_list.append(input_vessel_list[i])
		#获取实际的涉及港口集合
		for i in range(0,port_no):
			if i > 2:  #TS港口中的代码为英文，需转换为代码以保持与POL,POD,FND的一致
				port_list.append(port_dict[input_port_list[i].strip().upper()])
			else:
				port_list.append(input_port_list[i])
		
		#标示空干线航次
		for i in range(0,vessel_no):
			if vessel_list[i].strip() in ['- -','--']:               #若当前航次为虚拟空航次
				if i == 0:                                #代表当前处理的是第一段路径即起运港到第一中转港
					if (port_list[0] in baseport_dict.keys()) and (port_list[3] in baseport_dict.keys()) and (port_list[0]!=port_list[3]):   #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
						if (port_list[0] + '-' + port_list[3]) not in BASEPORT_ROUTE_EXCEPTION:  #排除基本港支线的特殊情况
							result = result + 'svvd1为虚拟空干线航次，路径为' + port_list[0] + '-' + port_list[3] + ';'
				elif i == vessel_no - 1:                  #代表当前处理的是最后一段路径即最后一中转港到卸港/最终目的港
					if  port_list[-1] == port_list[1]:     #若最后一中转港等于卸港，则最后一段路径取最后一中转港到最终目的港
						if (port_list[-1] in baseport_dict.keys()) and (port_list[2] in baseport_dict.keys()) and (port_list[-1]!=port_list[2]):   #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
							if (port_list[-1] + '-' + port_list[2]) not in BASEPORT_ROUTE_EXCEPTION: #排除基本港支线的特殊情况
								result = result + 'svvd'+ str(i+1) + '为虚拟空干线航次，路径为' + port_list[-1] + '-' + port_list[2] + ';'
					else:                                   #若最后一中转港不等于卸港，则最后一段路径取最后一中转港卸港到卸港
						if (port_list[-1] in baseport_dict.keys()) and (port_list[1] in baseport_dict.keys()) and (port_list[-1]!=port_list[1]):   #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
							if (port_list[-1] + '-' + port_list[1]) not in BASEPORT_ROUTE_EXCEPTION: #排除基本港支线的特殊情况
								result = result + 'svvd'+ str(i+1) + '为虚拟空干线航次，路径为' + port_list[-1] + '-' + port_list[1] + ';'
				else:                                     #代表当前处理的是中间路径即中转港到中转港
					if (port_list[i+2] in baseport_dict.keys()) and (port_list[i+3] in baseport_dict.keys()) and (port_list[i+2]!=port_list[i+3]):  #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
						if (port_list[i+2] + '-' + port_list[i+3]) not in BASEPORT_ROUTE_EXCEPTION: #排除基本港支线的特殊情况	
							result = result + 'svvd'+ str(i+1) + '为虚拟空干线航次，路径为' + port_list[i+2] + '-' + port_list[i+3] + ';'
		return result

def mainline_filter(cn,ct,ts,pol,v1,ts1,v2,ts2,v3,ts3,v4,pod,fnd,bl):
	'''
	用于标示主干线航次
	'''
	if int(ts) == 0:     #若无中转，则svvd1必定为主干线航次
		return v1
	elif bl.startswith('COSU'): #排除集运外贸提单
		return '集运外贸航次'
	else:
		input_port_list = [pol,pod,fnd,ts1,ts2,ts3] #所有舱单输入的涉及港口包含空 
		input_vessel_list = [v1,v2,v3,v4] #所有舱单输入的承载航次包含空
		vessel_no = int(ts)+1 #实际承载航次数为中转数加1
		port_no = int(ts)+3 #实际涉及港口数为中转数加3，即起运港，卸港，目的港加（中转港*中转次数）
		vessel_list = [] #用于存储实际承运航次
		port_list = [] #用于存储实际挂港	
		#获取实际的承载航次集合
		for i in range(0,vessel_no):
			vessel_list.append(input_vessel_list[i])
		#获取实际的涉及港口集合
		for i in range(0,port_no):
			if i > 2:  #TS港口中的代码为英文，需转换为代码以保持与POL CDE,POD CDE,FND CDE的一致
				port_list.append(port_dict[input_port_list[i].strip().upper()])
			else:
				port_list.append(input_port_list[i])

		#标示空干线航次
		for i in range(0,vessel_no):
			if vessel_list[i].strip() in ['- -','--']:               #若当前航次为虚拟空航次
				if i == 0:                                #代表当前处理的是第一段路径即起运港到第一中转港
					if (port_list[0] in baseport_dict.keys()) and (port_list[3] in baseport_dict.keys()) and (port_list[0]!=port_list[3]):   #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
						if (port_list[0] + '-' + port_list[3]) not in BASEPORT_ROUTE_EXCEPTION: #排除虎门/东江仓到南沙/高栏/大铲湾这种基本港支线的特殊情况
							vessel_list[i] = 'IC0-'        #用IC0标示虚拟空干线航次
				elif i == vessel_no - 1:                  #代表当前处理的是最后一段路径即最后一中转港到卸港/最终目的港
					if  port_list[-1] == port_list[1]:     #若最后一中转港等于卸港，则最后一段路径取最后一中转港到最终目的港
						if (port_list[-1] in baseport_dict.keys()) and (port_list[2] in baseport_dict.keys()) and (port_list[-1]!=port_list[2]):   #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
							if (port_list[-1] + '-' + port_list[2]) not in BASEPORT_ROUTE_EXCEPTION: #排除虎门/东江仓到南沙/高栏/大铲湾这种基本港支线的特殊情况
								vessel_list[i] = 'IC0-'       #用IC0标示虚拟空干线航次
					else:                                  #若最后一中转港不等于卸港，则最后一段路径取最后一中转港卸港到卸港
						if (port_list[-1] in baseport_dict.keys()) and (port_list[1] in baseport_dict.keys()) and (port_list[-1]!=port_list[1]):   #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
							if (port_list[-1] + '-' + port_list[1]) not in BASEPORT_ROUTE_EXCEPTION: #排除虎门/东江仓到南沙/高栏/大铲湾这种基本港支线的特殊情况
								vessel_list[i] = 'IC0-'       #用IC0标示虚拟空干线航次
				else:                                     #代表当前处理的是中间路径即中转港到中转港
					if (port_list[i+2] in baseport_dict.keys()) and (port_list[i+3] in baseport_dict.keys()) and (port_list[i+2]!=port_list[i+3]):  #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
						if (port_list[i+2] + '-' + port_list[i+3]) not in BASEPORT_ROUTE_EXCEPTION: #排除虎门/东江仓到南沙/高栏/大铲湾这种基本港支线的特殊情况
							vessel_list[i] = 'IC0-'          #用IC0标示虚拟空干线航次
		
		main_vsl_ports_dict = {} #用于存储干线航次和航次对应的挂港的字典
		#获取干线航次字典（附加挂港信息）
		for i in range(0,vessel_no):
			service = vessel_list[i].strip().split('-')[0] #获取航线名
			if service in MAINLINE: #若航线名在干线航次集合内则代表取到干线航次
				if i == 0:    #代表当前处理的是第一段路径即起运港到第一中转港
					main_vsl_ports_dict[vessel_list[i]] = port_list[0] + '-' + port_list[3]
				elif i == vessel_no - 1: #代表当前处理的是最后一段路径即最后一中转港到卸港/最终目的港
					if port_list[-1] == port_list[1]:   #若最后一中转港等于卸港，则最后一段路径取最后一中转港到最终目的港
						main_vsl_ports_dict[vessel_list[i]] = port_list[-1] + '-' + port_list[2]
					else:                               #若最后一中转港不等于卸港，则最后一段路径取最后一中转港卸港到卸港
						main_vsl_ports_dict[vessel_list[i]] = port_list[-1] + '-' + port_list[1]
				else:                                   #代表当前处理的是中间路径即中转港到中转港
					main_vsl_ports_dict[vessel_list[i]] = port_list[i+2] + '-' + port_list[i+3]
			elif service.startswith('CF'):  #若航线名以CF开头仅当子路径挂港均为基本港时才是（支线运营）干线航次,注意目前CF3为内贸运营的干线航次
				if i == 0:    #代表当前处理的是第一段路径即起运港到第一中转港
					if (port_list[0] in baseport_dict.keys()) and (port_list[3] in baseport_dict.keys()): #只有当前路径段起运港和目的港均为基本港时的CF航线才是干线
						main_vsl_ports_dict[vessel_list[i]] = port_list[0] + '-' + port_list[3]
				elif i == vessel_no - 1: #代表当前处理的是最后一段路径即最后一中转港到卸港/最终目的港
					if port_list[-1] == port_list[1]:   #若最后一中转港等于卸港，则最后一段路径取最后一中转港到最终目的港
						if (port_list[-1] in baseport_dict.keys()) and (port_list[2] in baseport_dict.keys()): #只有当前路径段起运港和目的港均为基本港时的CF航线才是干线
							main_vsl_ports_dict[vessel_list[i]] = port_list[-1] + '-' + port_list[2]
					else:                               #若最后一中转港不等于卸港，则最后一段路径取最后一中转港卸港到卸港
						if (port_list[-1] in baseport_dict.keys()) and (port_list[1] in baseport_dict.keys()): #只有当前路径段起运港和目的港均为基本港时的CF航线才是干线
							main_vsl_ports_dict[vessel_list[i]] = port_list[-1] + '-' + port_list[1]
				else:                                   #代表当前处理的是中间路径即中转港到中转港
					if (port_list[i+2] in baseport_dict.keys()) and (port_list[i+3] in baseport_dict.keys()): #只有当前路径段起运港和目的港均为基本港时的CF航线才是干线
						main_vsl_ports_dict[vessel_list[i]] = port_list[i+2] + '-' + port_list[i+3]

		if len(main_vsl_ports_dict) == 0:  #若干线航次字典为空则返回异常说明（理论上不应该出现）
			return '无干线航次？！'
		elif len(main_vsl_ports_dict) == 1:  #若干线航次字典长度为1则返回唯一的干线航次（字典的唯一键）
			return list(main_vsl_ports_dict.keys())[0]
		else:               #否则需在多个干线航次中判断主干线航次
			#main_ports_vsl_dict = {val:key for key,val in main_vsl_ports_dict.items()}  #将干线航次字典的键值互换存储在新字典中以备航线和挂港相互引用
			ml_vessels = [] #用于依序存储干线承运航次
			ml_ports = [] #用于依序存储干线航次挂港且和干线航次列表一一对应
			#将干线航次和挂港依序存储
			for vsl in vessel_list:
				if vsl in main_vsl_ports_dict.keys():
					ml_vessels.append(vsl)
					ml_ports.append(main_vsl_ports_dict[vsl])
			
			'''
			#判断是否有除了虚拟航次以外的同一航次重复出现
			ml_vessels_withoutIC0 = [x for x in ml_vessels if x!='IC0-'] #不考虑虚拟干线航次
			ml_vessels_withoutIC0_set = set(ml_vessels_withoutIC0) #取集合
			if len(ml_vessels_withoutIC0_set) != len(ml_vessels_withoutIC0):
				return '同一航次在该票货的不同路径重复出现，请检查！'
			'''
			
			pair_list = [] #用于存储干线子路径起运港区域和卸港区域是否一致的信息
			for i in range(0,len(ml_ports)):
				try:
					former = baseport_region_dict[ml_ports[i].split('-')[0]] #干线子路径起运港匹配区域
					latter = baseport_region_dict[ml_ports[i].split('-')[1]] #干线子路径卸港匹配区域
				except KeyError:
					return ml_ports[i].split('-')[0] + '或' + ml_ports[i].split('-')[1] + '无对应区域信息，请检查配置表'
				if former == latter:  #若干线子路径起运港区域和卸港区域一致，则赋值为0
					pair_list.append(0)
				else:                 #若干线子路径起运港区域和卸港区域不一致，则赋值为1
					pair_list.append(1)
			if pair_list.count(1) == 1: #当各干线子路径中只有一个子路径起运港区域和卸港区域不一致时（其余一致），取该不一致子路径承运航次为主干线航次
				return ml_vessels[pair_list.index(1)]
			elif pair_list.count(1) == 0: #当各干线子路径起运港区域和卸港区域都一致时，返回异常说明
				return '各干线航次挂港都在一个区域内，无法判断干支线！'
			
			else:                        #否则进行第二层次的匹配判断
				pair_list = []     #重置存储干线子路径起运港区域和卸港区域是否一致的信息的列表，用于存储各子路径起运港和卸港大区域信息
				for i in range(0,len(ml_ports)):
					try:
						former = baseport_highregion_dict[ml_ports[i].split('-')[0]] #干线子路径起运港匹配大区域
						latter = baseport_highregion_dict[ml_ports[i].split('-')[1]] #干线子路径卸港匹配大区域
					except KeyError:
						return ml_ports[i].split('-')[0] + '或' + ml_ports[i].split('-')[1] + '无对应大区域信息，请检查配置表'
					if former == latter:  #若干线子路径起运港大区域和卸港大区域一致，则赋值为0
						pair_list.append(0)				
					elif (former == '北' and latter == '南') or (former == '南' and latter == '北'): #若干线子路径起运港和卸港大区域为南北（北南）对则赋值为3
						pair_list.append(3)
					elif (former == '北' and latter == '中') or (former == '中' and latter == '北'): #若干线子路径起运港和卸港大区域为北中（中北）对则赋值为2
						pair_list.append(2)
					elif (former == '南' and latter == '中') or (former == '中' and latter == '南'): #若干线子路径起运港和卸港大区域为中南（南中）对则赋值为1
						pair_list.append(1)
				if pair_list.count(0) == len(ml_vessels):   #当各干线子路径起运港大区域和卸港大区域都一致时，进行进一步判断
					
					ml_port_region_sequences = '' #用于存储按顺序排列的干线航次挂港所在区域的序列（注意非挂港路径区域序列,即排除前后连续的重复港）
					try:
						#首先添加首个子路径的第一港的区域信息（无论如何都需要添加）
						ml_port_region_sequences = ml_port_region_sequences + baseport_region_dict[ml_ports[0].split('-')[0]]
						for j in range(0,len(ml_ports)):
							if j == len(ml_ports)-1: #若最后一个子路径则无论如何必须添加（且仅需添加）最后一港的区域信息
								ml_port_region_sequences = ml_port_region_sequences + baseport_region_dict[ml_ports[j].split('-')[1]]
							else:      #若非最后一个子路径则需进行连续路径港口是否重复的判断
								if ml_ports[j].split('-')[1] == ml_ports[j+1].split('-')[0]: #若前后子路径挂港连续则只添加前子路径的卸港的区域信息
									ml_port_region_sequences = ml_port_region_sequences + baseport_region_dict[ml_ports[j].split('-')[1]]
								else:    #若前后子路径挂港不连续则同时添加前子路径的卸港的区域信息和后子路径的装港的区域信息
									ml_port_region_sequences = ml_port_region_sequences + baseport_region_dict[ml_ports[j].split('-')[1]]
									ml_port_region_sequences = ml_port_region_sequences + baseport_region_dict[ml_ports[j+1].split('-')[0]]
					except KeyError:
						return '各干线航次挂港都在一个大区域内，无法判断干支线！(且有挂港无定义对应区域)'
					if ml_ports[0].split('-')[0] == 'SWA' and baseport_region_dict[ml_ports[0].split('-')[1]] == '福建': #若首段干线子路径的装港为汕头且卸港区域为福建则将汕头和福建视为一个区域指定次段干线子路径为主干线（含汕头或福建则默认都在南大区）
						return ml_vessels[1]
					elif ml_ports[-1].split('-')[1] == 'SWA' and baseport_region_dict[ml_ports[-1].split('-')[0]] == '福建': #若末段干线子路径的装港区域为福建且卸港为汕头则将汕头和福建视为一个区域指定倒数第二段干线子路径为主干线（含汕头或福建则默认都在南大区）
						return ml_vessels[-2]
					elif baseport_region_dict[ml_ports[0].split('-')[0]] == '福建' and baseport_region_dict[ml_ports[0].split('-')[1]] != '福建': #若首段干线子路径的装港区域为福建（且卸港区域不为福建）可确定首段干线子路径即含福建子路径为主干线（含福建则默认都在南大区）
						return ml_vessels[0]
					elif baseport_region_dict[ml_ports[-1].split('-')[1]] == '福建' and baseport_region_dict[ml_ports[-1].split('-')[0]] != '福建': #若末段干线子路径的卸港区域为福建（且装港区域不为福建）可确定末段干线子路径即含福建子路径为主干线（含福建则默认都在南大区）
						return ml_vessels[-1]
					elif ml_port_region_sequences in MAINLINE_DEFINED_REGION_RULE.keys(): #若挂港区域序列在人工定义规则列表中则按人工定义规则明示的主干线位置返回
						return ml_vessels[MAINLINE_DEFINED_REGION_RULE[ml_port_region_sequences]]
					else:
						return '各干线航次挂港都在一个大区域内，无法判断干支线！'
				elif 3 in pair_list:                        #按大区域南北（北南）优于北中（中北）优于中南（南中）的优先级进行主干线航次的判断
					if pair_list.count(3) == 1:             
						return ml_vessels[pair_list.index(3)]
					else:                                   #若同一大区域优先级的子路径个数大于1，返回异常说明
						return '存在多个跨大区域优先级相同（北南/南北）的干线航次！'
				elif 2 in pair_list:
					if pair_list.count(2) == 1:             
						return ml_vessels[pair_list.index(2)]
					else:                                   #若同一大区域优先级的子路径个数大于1，返回异常说明
						return '存在多个跨大区域优先级相同（北中/中北）的干线航次！'
				elif 1 in pair_list:
					if pair_list.count(1) == 1:             
						return ml_vessels[pair_list.index(1)]
					else:                                   #若同一大区域优先级的子路径个数大于1，返回异常说明
						return '存在多个跨大区域优先级相同（南中/中南）的干线航次！'
					
				
def arbd_route_filter(cn,ct,ts,pol,v1,ts1,v2,ts2,v3,ts3,v4,pod,fnd,ml,por):
	'''
	用于筛选出arb和ard的驳船路径
	'''
	#if int(ts) == 0:  #若无中转则无驳船（已废弃，考虑到不显式输入的北部湾驳船则无法应用此规则）
		#return ''
	if ml.startswith('CF') and ml.split('-')[0] != 'CF3': #若主干线为CF航向也无驳船（对于内贸来说）,注意CF3目前为内贸运营航次视为IC干线
		return ''
	else:
		input_port_list = [pol,pod,fnd,ts1,ts2,ts3] #所有舱单输入的涉及港口包含空 
		input_vessel_list = [v1,v2,v3,v4] #所有舱单输入的承载航次包含空
		vessel_no = int(ts)+1 #实际承载航次数为中转数加1
		port_no = int(ts)+3 #实际涉及港口数为中转数加3，即起运港，卸港，目的港加（中转港*中转次数）
		vessel_list = [] #用于存储实际承运航次
		port_list = [] #用于存储实际挂港
		result = ''  #用于存储路径提示信息
		#获取实际的承载航次集合
		for i in range(0,vessel_no):
			vessel_list.append(input_vessel_list[i])
		#获取实际的涉及港口集合
		for i in range(0,port_no):
			if i > 2:  #TS港口中的代码为英文，需转换为代码以保持与POL,POD,FND的一致
				port_list.append(port_dict[input_port_list[i].strip().upper()])
			else:
				port_list.append(input_port_list[i])

		#标示空干线航次
		for i in range(0,vessel_no):
			if vessel_list[i].strip() in ['- -','--']:               #若当前航次为虚拟空航次
				if i == 0:                                #代表当前处理的是第一段路径即起运港到第一中转港
					if (port_list[0] in baseport_dict.keys()) and (port_list[3] in baseport_dict.keys()) and (port_list[0]!=port_list[3]):   #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
						if (port_list[0] + '-' + port_list[3]) not in BASEPORT_ROUTE_EXCEPTION: #排除虎门/东江仓到南沙/高栏/大铲湾这种基本港支线的特殊情况
							vessel_list[i] = 'IC0-'        #用IC0标示虚拟空干线航次
				elif i == vessel_no - 1:                  #代表当前处理的是最后一段路径即最后一中转港到卸港/最终目的港
					if  port_list[-1] == port_list[1]:     #若最后一中转港等于卸港，则最后一段路径取最后一中转港到最终目的港
						if (port_list[-1] in baseport_dict.keys()) and (port_list[2] in baseport_dict.keys()) and (port_list[-1]!=port_list[2]):   #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
							if (port_list[-1] + '-' + port_list[2]) not in BASEPORT_ROUTE_EXCEPTION: #排除虎门/东江仓到南沙/高栏/大铲湾这种基本港支线的特殊情况
								vessel_list[i] = 'IC0-'       #用IC0标示虚拟空干线航次
					else:                                   #若最后一中转港不等于卸港，则最后一段路径取最后一中转港卸港到卸港
						if (port_list[-1] in baseport_dict.keys()) and (port_list[1] in baseport_dict.keys()) and (port_list[-1]!=port_list[1]):   #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
							if (port_list[-1] + '-' + port_list[1]) not in BASEPORT_ROUTE_EXCEPTION: #排除虎门/东江仓到南沙/高栏/大铲湾这种基本港支线的特殊情况
								vessel_list[i] = 'IC0-'       #用IC0标示虚拟空干线航次
				else:                                     #代表当前处理的是中间路径即中转港到中转港
					if (port_list[i+2] in baseport_dict.keys()) and (port_list[i+3] in baseport_dict.keys()) and (port_list[i+2]!=port_list[i+3]):  #若当前路径段起运港和目的港均为基本港，则该虚拟空航次为干线
						if (port_list[i+2] + '-' + port_list[i+3]) not in BASEPORT_ROUTE_EXCEPTION: #排除虎门/东江仓到南沙/高栏/大铲湾这种基本港支线的特殊情况
							vessel_list[i] = 'IC0-'          #用IC0标示虚拟空干线航次
						
		i = 0 #用于记录涉及港口的位置
		for vsl in vessel_list:
			'''
			if vsl.split('-')[0] == '':                   #若匹配到虚拟空航次，则代表是驳船且当前承载航次路径为最后一段路径即最后一个中转港到卸港/最终目的港
				if port_list[-1] == port_list[1]:     #若最后一中转港等于卸港，则最后一段路径取最后一卸港到最终目的港
					result = result + port_list[-1]+'-'+port_list[2]+','
				else:
					result = result + port_list[-1]+'-'+port_list[1]+','
			'''
			if vsl.split('-')[0] in MAINLINE:           #若匹配到干线航次则当前位置数加一并继续下一次循环(还暗含匹配到没有中转港的直达航次即唯一航次就是干线)
				i = i + 1
				continue
			else:                                         #若未匹配到干线航次则代表当前匹配到驳船航次
				if i == 0:                                #代表当前处理的是第一段路径即起运港到第一中转港
					result = result +port_list[0]+'-'+port_list[3]+','
					i = i + 1
				elif i == vessel_no - 1:                  #代表当前处理的是最后一段路径即最后一中转港到卸港/最终目的港
					if  port_list[-1] == port_list[1]:     #若最后一中转港等于卸港，则最后一段路径取最后一中转港到最终目的港
						result = result + port_list[-1]+'-'+port_list[2]+','
					else:                                  #若最后一中转港不等于卸港，则最后一段路径取最后一中转港卸港到卸港
						result = result + port_list[-1]+'-'+port_list[1]+','
					break
				else:                                     #代表当前处理的是中间路径即中转港到中转港
					result = result + port_list[i+2]+'-'+port_list[i+3]+','
					i = i + 1
		
		#对不显式输入的北海、铁山、防城到钦州（或相反流向）的驳船路径进行匹配
		#arb部分
		if por == 'BHY' or pol == 'BHY':
			if 'BHY-QZH' not in result:  #注意如对应路径已显式输入匹配到则无需再重复添加
				result = 'BHY-QZH,'+ result #将路径添加到最前面
		elif por == 'TIE' or pol == 'TIE':
			if 'TIE-QZH' not in result:
				result = 'TIE-QZH,'+ result		
		elif por == 'FAN' or pol == 'FAN':
			if 'FAN-QZH' not in result:
				result = 'FAN-QZH,'+ result
		#ard部分
		if pod == 'BHY' or fnd == 'BHY': 
			if 'QZH-BHY' not in result: #注意如对应路径已显式输入匹配到则无需再重复添加
				result = result + 'QZH-BHY,'#将路径添加到最后面
		elif pod == 'TIE' or fnd == 'TIE':
			if 'QZH-TIE' not in result:
				result = result + 'QZH-TIE,'	
		elif pod == 'FAN' or fnd == 'FAN':
			if 'QZH-FAN' not in result:
				result = result + 'QZH-FAN,'
		
		return result
						
def arbd_route_match_filter(arbd,ct):
	'''
	用于匹配筛选出来的arb和ard驳船路径的费率
	'''
	if ct.startswith('2'):    #20尺非标箱暂用20尺标箱费率      
		arbd_dict = arbd_dict20
	elif ct.startswith('4'):    #40尺非标箱暂用40尺标箱费率
		arbd_dict = arbd_dict40
	'''
	if ct == '20尺标':          #使用20尺费率字典
		arbd_dict = arbd_dict20
	elif ct == '40尺标':        #使用40尺费率字典
		arbd_dict = arbd_dict40
	else:	
		return 0
	'''	
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
				#amount = 0
				#info = routes[0] + '未匹配到费率；'
				if routes[0] in arbd_replaced_dict.keys():  #若驳船路径在替换路径表中存在则进行替换并匹配费率
					amount = arbd_dict[arbd_replaced_dict[routes[0]]]
				return amount#,info
		elif len(routes) > 1:
			i = 0
			while i<len(routes):
				if (i<len(routes)-2) and (routes[i].split('-')[1] == routes[i+1].split('-')[0]) and (routes[i+1].split('-')[1] == routes[i+2].split('-')[0]): #若在倒数第二层路径之前且连续两对前后层路径相连则匹配三层路径的总起运港和总卸港
					try:
						amount = amount + arbd_dict[(routes[i].split('-')[0]+'-'+ routes[i+2].split('-')[1])]
						i = i + 3 #若匹配到三层路径则下次循环跳过后两程路径
					except KeyError:  #若三层连续路径无费率则不改变重新进行两层连续路径的匹配
						try:
							amount = amount + arbd_dict[(routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1])]
							i = i + 2 #若匹配到两层路径则下次循环跳过后程路径
						#info = info + '两层连续驳船路径；'
						except KeyError:
							#amount = 0
							try:  #若匹配不到两层连续路径的联程费率则匹配路径的各自费率-前半部分
								amount = amount + arbd_dict[routes[i]]
								i = i + 1
							except KeyError:
								try:  #若匹配不到两层连续路径的联程费率则匹配路径的各自费率-后半部分
									amount = amount + arbd_dict[routes[i+1]]
									i = i + 2
								except KeyError:
									i = i + 2
							#info = '两层连续驳船路径'+ routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1] + '未匹配到费率；'
				elif (i<len(routes)-1) and (routes[i].split('-')[1] == routes[i+1].split('-')[0]): #若非最后一层路径且前后层路径相连则匹配两层路径的总起运港和总卸港
					try:
						amount = amount + arbd_dict[(routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1])]
						i = i + 2 #若匹配到两层路径则下次循环跳过后程路径
						#info = info + '两层连续驳船路径；'
					except KeyError:
						#amount = 0
						try:  #若匹配不到两层连续路径的联程费率则匹配路径的各自费率-前半部分
							amount = amount + arbd_dict[routes[i]]
							i = i + 1
						except KeyError:
							try:  #若匹配不到两层连续路径的联程费率则匹配路径的各自费率-后半部分
								amount = amount + arbd_dict[routes[i+1]]
								i = i + 2
							except KeyError:
								i = i + 2						
							#info = '两层连续驳船路径'+ routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1] + '未匹配到费率；'
				else:
					try:
						amount = amount + arbd_dict[routes[i]]
						i = i + 1
					except KeyError:
						#amount = 0
						if routes[i] in arbd_replaced_dict.keys():  #若驳船路径在替换路径表中存在则进行替换并匹配费率
							amount = amount + arbd_dict[arbd_replaced_dict[routes[i]]]
						i = i + 1
						#info = routes[i] + '未匹配到费率；'
			return amount#,info

def arbd_route_info_filter(arbd,ct):
	'''
	用于是否匹配到arb和ard驳船路径的信息
	'''
	if ct.startswith('2'):    #20尺非标箱暂用20尺标箱费率      
		arbd_dict = arbd_dict20
	elif ct.startswith('4'):    #40尺非标箱暂用40尺标箱费率
		arbd_dict = arbd_dict40
	'''
	if ct == '20尺标':
		arbd_dict = arbd_dict20
	elif ct == '40尺标':
		arbd_dict = arbd_dict40	
	else:
		return '非标准箱型'
	'''	
	info = ''
	if arbd == '': #若手工ARBARD路径为空则直接返回        
		return '无驳船路径'
	else:
		routes = arbd[0:-1].split(',') #去除路径字符串末尾的分隔符，同时将多层路径（如果存在）以逗号分隔存储在列表中
		if len(routes) == 1:	 #若只有一层路径则直接匹配费率
			try:
				arbd_dict[routes[0]]
				return '一程驳船路径'
			except KeyError:
				info = routes[0] + '未匹配到费率；'
				if routes[0] in arbd_replaced_dict.keys(): #若驳船路径在替换路径表中存在则增加路径替换说明
					info = info  + '但' + routes[0] + '已进行驳船路径替换为' + arbd_replaced_dict[routes[0]] + '并匹配费率；'
				return info
		elif len(routes) > 1:
			i = 0
			while i<len(routes):
				if (i<len(routes)-2) and (routes[i].split('-')[1] == routes[i+1].split('-')[0]) and (routes[i+1].split('-')[1] == routes[i+2].split('-')[0]): #若在倒数第二层路径之前且连续两对前后层路径相连则匹配三层路径的总起运港和总卸港
					try:
						arbd_dict[(routes[i].split('-')[0]+'-'+ routes[i+2].split('-')[1])]
						i = i + 3 #若匹配到三层路径则下次循环跳过后两程路径
						info = info + '三程连续驳船路径；'
					except KeyError:
						info = info + '三程连续驳船路径'+ routes[i].split('-')[0]+'-'+ routes[i+2].split('-')[1] + '未匹配到费率；'
						i = i + 1 #！！！暂时加1！！！
				elif (i<len(routes)-1) and (routes[i].split('-')[1] == routes[i+1].split('-')[0]): #若非最后一层路径且前后层路径相连则匹配两层路径的总起运港和总卸港
					try:
						arbd_dict[(routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1])]
						i = i + 2 #若匹配到两层路径则下次循环跳过后程路径
						info = info + '两程连续驳船路径；'
					except KeyError:
						try:
							arbd_dict[routes[i]]
							arbd_dict[routes[i+1]]
							info = info + '两程连续驳船路径'+ routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1] + '未匹配到费率，但能分别匹配到连续驳船各自路径的费率；'
							i = i + 2
						except KeyError:
							info = info + '两程连续驳船路径'+ routes[i].split('-')[0]+'-'+ routes[i+1].split('-')[1] + '未匹配到费率，且不能全部匹配到连续驳船各自路径的费率；'
							i = i + 2
				else:
					try:
						arbd_dict[routes[i]]
						i = i + 1
						info = info + '多层非连续驳船路径'
					except KeyError:
						info = info + routes[i] + '未匹配到费率；'
						if routes[i] in arbd_replaced_dict.keys():  #若驳船路径在替换路径表中存在则增加路径替换说明
							info = info + '但' + routes[i] + '已进行驳船路径替换为' + arbd_replaced_dict[routes[i]] + '并匹配费率；'
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

def input_arbd_filter(charge_type,charge_amount,ib_intermodal,ob_intermodal):
	'''
	获取系统输入的ARB或ARD费用
	'''
	if charge_type == 'ARB':  #若费用代码为ARB且出口联运项包含rail则金额置为零
		if 'RAIL' in str(ob_intermodal).upper():
			return 0
		else:                 #否则获取金额
			return charge_amount
	elif charge_type == 'ARD':  #若费用代码为ARD且进口联运项包含rail则金额置为零
		if 'RAIL' in str(ib_intermodal).upper():
			return 0
		else:                 #否则获取金额
			return charge_amount
	else:
		return 0

def svvd_formatter(msvvd):
	'''
	将主干线航次格式化为不含-的形式
	'''
	svvdstr = ''
	strlist = msvvd.split('-')
	for str in strlist:
		svvdstr = svvdstr + str
	return svvdstr

def svvd_match(svvd,rmsvvd):
	'''
	将船名航次和修改后的主干线航次比较以标识提单箱量记录
	'''
	if svvd == rmsvvd:
		return 1
	else:
		return 0

def cy_cy_charge(arbd,arbd_included_cy_cy_charge):
	'''
	计算CY-CY运费
	'''
	return (arbd_included_cy_cy_charge - arbd)

def normal_ct_format(ct):
	'''
	统一标准箱代码以便后续计算，将20HQ统一修改为20GP,40GP统一修改为40HQ
	'''
	if ct == '20HQ':
		return '20GP'
	elif ct == '40GP':
		return '40HQ'
	else:
		return ct
		
time_start=time.time() #开始总运行时间的计时
	
filenames = os.listdir(FILES_DIR) #遍历文件夹获取所有文件的文件名

#遍历处理文件夹下所有文件
for filename in filenames:
	full_filename = FILES_DIR + '/' + filename
	df = pd.read_excel(full_filename)
	df['主干线航次改'] = df.apply(lambda x:svvd_formatter(msvvd=x['主干线航次']), axis=1)
	df['CY-CY含驳费用'] = df['CY-CY含驳费用'].fillna(0)
	df['SOC空'] = df['SOC空'].fillna('')
	#df['提单箱量标识'] = df.apply(lambda x:svvd_match(svvd=x['船名航次'],rmsvvd=x['主干线航次改']), axis=1)
	#bl_df = df[df['提单箱量标识'] == 1]
	bl_df = df[df['船名航次'] == df['主干线航次改']] #取提单箱量
	ubl_df = bl_df[bl_df['箱量TEU'] != 0] #取TEU不为0的记录（防止驳船费重复计费）
	ubl_df = ubl_df[ubl_df['CY-CY含驳费用'] != 0] #删除CY-CY含驳费用为0（未输入运费）的记录
	ubl_df = ubl_df[ubl_df['SOC空'] != 'Y'] #删除SOC空箱
	ubl_df = ubl_df[['船名航次','起运港/区域','卸港/区域','CNTR TYPE','箱量TEU','箱量UNIT','手工ARBARD费用','CY-CY含驳费用']]
	ubl_df['CY-CY运价'] =  ubl_df.apply(lambda x:cy_cy_charge(arbd=x['手工ARBARD费用'],arbd_included_cy_cy_charge=x['CY-CY含驳费用']),axis=1) #计算CY-CY运价
	ubl_df['箱型'] = ubl_df.apply(lambda x:normal_ct_format(ct=x['CNTR TYPE']),axis=1) #统一标准箱的箱型代码
	polpod_result = pd.pivot_table(ubl_df,values=['箱量UNIT','箱量TEU','CY-CY运价'],index=['船名航次','起运港/区域','卸港/区域','箱型'],aggfunc=np.sum)
	polpod_result = pd.DataFrame(polpod_result,columns=['箱量UNIT','箱量TEU','CY-CY运价']) #对输出按指定排序
	svvd_result = pd.pivot_table(ubl_df,values=['箱量UNIT','箱量TEU','CY-CY运价'],index=['船名航次','箱型'],aggfunc=np.sum)
	svvd_result = pd.DataFrame(svvd_result,columns=['箱量UNIT','箱量TEU','CY-CY运价']) #对输出按指定排序
	writer = pd.ExcelWriter(OUTPUT_ADDR + '/' + filename + '运价统计.xlsx')
	svvd_result.to_excel(writer,'分航次运价统计',merge_cells=False,index=True)
	polpod_result.to_excel(writer,'分航线分流向运价统计',merge_cells=False,index=True)

time_end=time.time() #结束总运行时间的计时
print('本次运行用时共计'+str(int(time_end-time_start))+'秒。')
