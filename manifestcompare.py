DOWNLOADED_FILES_DIR = 'E:/Python35/code/manifest/downloadexcel'
UPDATE_FILES_DIR = 'E:/Python35/code/manifest/updateexcel'

import os
import pandas as pd
from pandas import Series,DataFrame
import numpy as np
import time
import hashlib
import shutil

time_start=time.time() #开始总运行时间的计时

ans = input('请输入舱单所属周次:（格式为yyyyww）')
week = str(ans)
EXISTED_FILES_DIR = 'D:/CSCL/Documents/工作/收益管理/测算预估/测算舱单共享/' + week + '/原始舱单'

d_filenames = os.listdir(DOWNLOADED_FILES_DIR) #获取新下载文件夹获取所有新下载舱单文件的文件名
e_filenames = os.listdir(EXISTED_FILES_DIR) #获取已下载文件夹获取所有已下载舱单文件的文件名

for d_filename in d_filenames: #遍历新下载文件夹各个文件的文件名
	if d_filename in e_filenames: #若该新下载文件的文件名已经在已下载文件夹中存在则进行文件的md5值对比
		fd = open(DOWNLOADED_FILES_DIR + '/' + d_filename,'rb')
		d_contents = fd.read()
		fd.close()
		d_md5 = hashlib.md5(d_contents).hexdigest() #获取新下载文件的md5值

		fe = open(EXISTED_FILES_DIR + '/' + d_filename,'rb')
		e_contents = fe.read()
		fe.close()
		e_md5 = hashlib.md5(e_contents).hexdigest() #获取已下载同名文件的md5值
		
		if d_md5 == e_md5: #当两文件md5值相同时代表文件内容无变化没有更新，不做任何操作
			continue
		else: #当两文件md5值不同时代表文件内容有变化需要更新，因此复制新下载文件到需更新舱单文件夹
			print(d_filename + '文件内容有更新！')
			shutil.copyfile(os.path.join(DOWNLOADED_FILES_DIR,d_filename),os.path.join(UPDATE_FILES_DIR,d_filename))
	else: #否则直接复制新下载文件到需更新舱单文件夹
		print(d_filename + '文件尚不存在！')
		shutil.copyfile(os.path.join(DOWNLOADED_FILES_DIR,d_filename),os.path.join(UPDATE_FILES_DIR,d_filename))
		continue

time_end=time.time() #结束总运行时间的计时
print('本次运行用时共计'+str(int(time_end-time_start))+'秒。')