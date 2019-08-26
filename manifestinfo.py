FILES_DIR = r'E:\Python35\code\manifest\manifestexcel'
PARA_FILE = r'E:\Python35\code\manifest\parameter.xls'
OUTPUT_FILE_ADDR = r'E:\Python35\code\manifest\output.csv'

import os
import pandas as pd
from pandas import Series,DataFrame
import numpy as np
import time


def zero_buc_corrector(teu,buc,comm_related_charge):
	'''
	将TEU不为0而BUC舱单原值为0的记录进行舱单运输类收入总和科目金额的更改
	'''
	if teu == 0: 
		return comm_related_charge
	elif teu == 1:   #若BUC为0且TEU不为0则根据值（1代表小箱，2代表大箱）进行调整
		if buc == 0:
			return comm_related_charge - 300
		else:
			return comm_related_charge
	elif teu == 2:
		if buc == 0:
			return comm_related_charge - 400
		else:
			return comm_related_charge
	else:
		return "箱型有误！"

def refund_ibs_filter(ibs,ibstariff):
	'''
	计算IBS返还值
	'''
	if ibstariff == -9999 or ibstariff == -8888 or ibstariff == -7777: #代表未匹配到IBS返还费率
		return 0
	else:
		return min(ibs,ibstariff) #返回实收费率和返还费率的较小值
		
time_start=time.time() #开始总运行时间的计时
	
filenames = os.listdir(FILES_DIR) #遍历文件夹获取所有文件的文件名
f = open(OUTPUT_FILE_ADDR,'w+') #记录输出结果
f.write('SVVD,重箱量,提单箱量,SOC箱量,SOC空箱量,舱单收入,CY-CY收入,数字抬头收入,ARB+ARD,IHL+IHD,小箱UNIT,大箱UNIT,DOC\DCI\SLF,TSD,铁驳费用,手工驳船费,CY-CY含驳,LOCAL CHARGE,BUC,ICD,PSU,DPS,BKF,IBS,IBS返还,电商7开头费用,揽货佣金,景华峰操作费,船代佣金,未输入运费箱量,拖车箱量\n')

#遍历处理文件夹下所有文件
for filename in filenames:
	full_filename = FILES_DIR + '/' + filename
	df = pd.read_excel(full_filename)
	df['SOC'] = df['SOC'].fillna('') #填充空值以避免类型转换问题
	df['TERMS'] = df['TERMS'].fillna('') #填充空值以避免类型转换问题
	df['SOC空'] = df['SOC空'].fillna('') #填充空值以避免类型转换问题
	df['TTL AMT'] = df['TTL AMT'].fillna(0) #填充空值
	df['CY-CY含驳费用'] = df['CY-CY含驳费用'].fillna(0) #填充空值
	df['箱量TEU'] = df['箱量TEU'].fillna(0) #填充空值
	df['BUC舱单原值'] = df['BUC舱单原值'].fillna(0) #填充空值
	df['舱单运输类收入总和（含拖车和铁路）'] = df['舱单运输类收入总和（含拖车和铁路）'].fillna(0) #填充空值
	df['舱单运输类收入总和（含拖车和铁路）改'] = df.apply(lambda x:zero_buc_corrector(teu=x['箱量TEU'],buc=x['BUC舱单原值'],comm_related_charge=x['舱单运输类收入总和（含拖车和铁路）']), axis=1) #计算箱量非0而BUC为0情况下舱单运输类收入总和（含拖车和铁路）的更改
	df['IBS返还值'] = df.apply(lambda x:refund_ibs_filter(ibs=x['IBS舱单原值'],ibstariff=x['IBS返还费率']), axis=1)
	svvdset = set(df['船名航次'])
	#遍历当前文件中的各个航次
	for svvd in svvdset:
		cdf = df[df['船名航次'] == svvd] #只取当前处理的svvd信息
		total_teu = cdf['箱量TEU'].sum() #当前航次舱单重箱量
		#获取当前航次并转变航次格式
		if len(cdf['船名航次'].unique()[0]) == 10:
			trunkname = cdf['船名航次'].unique()[0][0:3] + '-' + cdf['船名航次'].unique()[0][3:6] + '-' + cdf['船名航次'].unique()[0][6:]
		elif len(cdf['船名航次'].unique()[0]) == 11:
			trunkname = cdf['船名航次'].unique()[0][0:4] + '-' + cdf['船名航次'].unique()[0][4:7] + '-' + cdf['船名航次'].unique()[0][7:]		
		
		bl_df = cdf[cdf['主干线航次'] == trunkname] #只取当前处理的svvd的提单箱量信息
		d2candc2d_df = bl_df[bl_df['TERMS'].isin(['Door-CY','CY-Door'])] #只取单端为门条款的提单箱量信息
		d2d_df = bl_df[bl_df['TERMS'] == 'Door-Door'] #只取双端为门条款的提单箱量信息
		truckteu = d2candc2d_df['箱量TEU'].sum() + d2d_df['箱量TEU'].sum()*2 #以单端为门条款视为一次拖车，双端为门条款视为二次拖车的原则计算拖车箱量
		zero_freight_teu = bl_df[bl_df['TTL AMT'] == 0]['箱量TEU'].sum() #计算未输入运费的提单箱量
		bl_teu = bl_df['箱量TEU'].sum() #计算提单箱量
		bl_20_unit = bl_df[bl_df['箱型2'] == 2]['箱量UNIT'].sum() #计算小箱unit数
		bl_40_unit = bl_df[bl_df['箱型2'] == 4]['箱量UNIT'].sum() #计算大箱unit数
		soc_teu = bl_df[bl_df['SOC'] == 'SOC']['箱量TEU'].sum() #计算soc箱量
		esoc_teu = bl_df[bl_df['SOC空'] == 'Y']['箱量TEU'].sum() #计算soc空箱量
		ttl_amount = bl_df['TTL AMT'].sum() #计算舱单收入
		manual_arbd_amount =  bl_df['手工ARBARD费用'].sum() #计算手工驳船费
		rail_arbd_amount =  bl_df['铁路费用舱单原值'].sum() #计算铁驳
		arbd_amount = manual_arbd_amount + rail_arbd_amount #计算合计驳船费
		ihld_amount = bl_df['IHL/IHD舱单原值'].sum() #计算拖车费
		cy_cy_plus_arbd = bl_df['CY-CY含驳费用'].sum() #计算CY-CY含驳费用(不含铁驳)
		tsd = bl_df['TSD舱单原值'].sum() #计算TSD费用
		rttl_amount = ttl_amount - tsd #计算输出用的舱单收入
		icd = bl_df['ICD舱单原值'].sum() #计算ICD费用
		buc = bl_df['BUC舱单原值'].sum() #计算BUC费用
		dds = bl_df['DOC\DCI\SLF'].sum() #计算DOC\DCI\SLF费用
		epanasia_num_charge = bl_df['电商CHARGE'].sum() #计算电商数字费用7EC,7EI,7EO,7EN
		psu = bl_df['PSU舱单原值'].sum() #计算PSU费用
		dps = bl_df['DPS舱单原值'].sum() #计算DPS费用
		bkf = bl_df['BKF舱单原值'].sum() #计算BKF费用
		ibs = bl_df['IBS舱单原值'].sum() #计算IBS费用
		ribs = bl_df['IBS返还值'].sum() #计算返还IBS费用
		local_charge = psu + dps + bkf + ibs #计算local charge费用
		num_initial_charge = ttl_amount - cy_cy_plus_arbd - rail_arbd_amount - ihld_amount - local_charge - tsd #计算数字开头的费用（倒推）
		cy_cy_charge = ttl_amount - num_initial_charge - arbd_amount - ihld_amount - local_charge - tsd #计算CY-CY费用（倒推）
		included_y_cdyj = (bl_df[bl_df['有Y标识的佣金舱单总收入'] != 0]['OCB舱单原值'].sum() +  bl_df[bl_df['有Y标识的佣金舱单总收入'] != 0]['有Y标识的佣金舱单总收入'].sum())*0.7*0.00475*1.06 #计算include含Y标识且特定charge不为0的佣金
		included_n_cdyj = bl_df[bl_df['有Y标识的佣金舱单总收入'] == 0]['OCB舱单原值'].sum()*0.00475*1.06#计算include含Y标识为0的佣金
		cdyj = included_y_cdyj + included_n_cdyj #计算船代佣金
		epanasia_df = bl_df[bl_df['电商'] == 'Y'] #电商记录集
		non_epanasia_df = bl_df[bl_df['电商'] == 'N'] #线下记录集
		wuh_non_epanasia_df = non_epanasia_df[non_epanasia_df['起运港/区域'] == '长江中上游'] #POL为武汉区域的线下记录集
		non_wuh_non_epanasia_df = non_epanasia_df[non_epanasia_df['起运港/区域'] != '长江中上游'] #POL为非武汉区域的线下记录集		
		#根据主干线航次的航向来决定电商货和POL为非武汉区域的线下货的揽货佣金
		if svvd[-1].upper() in ['S','E']:
			elhyj = epanasia_df['舱单运输类收入总和（含拖车和铁路）改'].sum() * 0.017 * 1.06 * 0.6 #计算南行航次的电商货佣金
			non_wuh_nelhyj = non_wuh_non_epanasia_df['舱单运输类收入总和（含拖车和铁路）改'].sum() * 0.017 * 1.06 #计算南行航次下POL不为武汉区域的线下货佣金
		elif svvd[-1].upper() in ['N','W']:
			elhyj = epanasia_df['舱单运输类收入总和（含拖车和铁路）改'].sum() * 0.038 * 1.06 * 0.6 #计算北行航次的电商货佣金
			non_wuh_nelhyj = non_wuh_non_epanasia_df['舱单运输类收入总和（含拖车和铁路）改'].sum() * 0.038 * 1.06 #计算北行航次下POL不为武汉区域的线下货佣金
		else:
			print('%s航向不为北或南'%(svvd))
		#n_elhyj = epanasia_df[epanasia_df['流向'] == '北行']['舱单运输类收入总和（含拖车和铁路）改'].sum() * 0.04 * 0.6 #计算北行电商货佣金(看单票货流)
		#s_elhyj = epanasia_df[epanasia_df['流向'] == '南行']['舱单运输类收入总和（含拖车和铁路）改'].sum() * 0.018 * 0.6 #计算南行电商货佣金(看单票货流)
		wuh_nelhyj = 28.5 * wuh_non_epanasia_df['箱量TEU'].sum() #计算POL为武汉区域线下佣金（含南北行）
		#n_non_wuh_nelhyj = non_wuh_non_epanasia_df[non_wuh_non_epanasia_df['流向'] == '北行']['舱单运输类收入总和（含拖车和铁路）改'].sum() * 0.04 #计算POL不为武汉区域的线下北行货佣金(看单票货流)
		#s_non_wuh_nelhyj = non_wuh_non_epanasia_df[non_wuh_non_epanasia_df['流向'] == '南行']['舱单运输类收入总和（含拖车和铁路）改'].sum() * 0.018 #计算POL不为武汉区域的线下南行货佣金(看单票货流)
		jy_lhyj = 1 * bl_teu #计算集运佣金
		lhyj = elhyj + wuh_nelhyj + non_wuh_nelhyj + jy_lhyj #计算合计揽货佣金
		#lhyj = n_elhyj + s_elhyj + wuh_nelhyj + n_non_wuh_nelhyj + s_non_wuh_nelhyj + jy_lhyj #计算合计揽货佣金(看单票货流)
		epanasia_czf =  bl_df[bl_df['电商'] == 'Y']['箱量TEU'].sum() *30 #计算景华峰(电商箱量)操作费，费率为30万箱量以下30，以上15
		
		#输出结果
		f.write(svvd+','+str(int(round(total_teu,0)))+','+str(int(round(bl_teu,0)))+','+str(int(round(soc_teu,0)))+','+str(int(round(esoc_teu,0)))+','
		+str(int(round(rttl_amount,0)))+','+str(int(round(cy_cy_charge,0)))+','+str(int(round(num_initial_charge,0)))+','+str(int(round(arbd_amount,0)))+','+str(int(round(ihld_amount,0)))+','
		+str(int(round(bl_20_unit,0)))+','+str(int(round(bl_40_unit,0)))+','+str(int(round(dds,0)))+','+str(int(round(tsd,0)))+','+str(int(round(rail_arbd_amount,0)))+','
		+str(int(round(manual_arbd_amount,0)))+','+str(int(round(cy_cy_plus_arbd,0)))+','+str(int(round(local_charge,0)))+','+str(int(round(buc,0)))+','+str(int(round(icd,0)))+','
		+str(int(round(psu,0)))+','+str(int(round(dps,0)))+','+str(int(round(bkf,0)))+','+str(int(round(ibs,0)))+','+str(int(round(ribs,0)))+','+str(int(round(epanasia_num_charge,0)))+','
		+str(int(round(lhyj,0)))+','+str(int(round(epanasia_czf,0)))+','+str(int(round(cdyj,0)))+','+str(int(round(zero_freight_teu,0)))+','+str(int(round(truckteu,0)))+'\n')

f.close()

time_end=time.time() #结束总运行时间的计时
print('本次运行用时共计'+str(int(time_end-time_start))+'秒。')
