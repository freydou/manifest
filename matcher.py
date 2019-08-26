FILES_DIR = r'E:\Python35\code\manifest\excel'
PARA_FILE = r'E:\Python35\code\manifest\tpara.xls'
DIGITAL = ['0','1','2','3','4','5','6','7','8','9']
NON_CY_CHRG_TYPE = ['IHL','IHD','TSD']

import os
import pandas as pd
from pandas import Series,DataFrame
import numpy as np

def matcher():
	'''
	读取参数excel文件获取各个匹配字典
	'''
	dfbarge = pd.read_excel(PARA_FILE,sheetname='barge',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictbarge = dfbarge.to_dict(orient='dict')
	dfbp = pd.read_excel(PARA_FILE,sheetname='baseport',index_col=0) #注意需明示不使用默认索引，使用第一列为索引
	dictbp = dfbp.to_dict(orient='dict')
	
	return dictbarge,dictbp


barge_dict,bp_dict = matcher() #获取匹配字典
#print(repr(barge_dict))
print(repr(bp_dict['区域']))