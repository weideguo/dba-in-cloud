# -*- coding: utf-8 -*-
#
# 一些基础函数集合
# weideguo@dba 20210712
#
import os
import re
import time
import shutil
import requests
import datetime
from contextlib import closing
from traceback import format_exc

import pytz
import gevent


def download_file(url, path, filename_pre="", filename="", temp_postfix=".temp", *args, **kwargs):
    """带有超时检测"""
    connect_timeout = 10
    read_timeout = 3600    
    with closing(requests.get(url, stream=True, timeout=(connect_timeout, read_timeout))) as response:
        chunk_size = 1024                                                       # 单次持久化最大值
        content_size = int(response.headers["content-length"])                  # 内容体总大小
        if not filename:
            filename = response.headers["Content-Disposition"].split("filename=\"")[-1].split("\"")[0]
            filename = filename.encode("ISO-8859-1").decode("utf-8")            # header默认用ISO-8859-1编码
            filename = filename_pre+filename
        
        filename = os.path.join(path, filename)
        if path:
            os.makedirs(path, exist_ok=True)
        
        _filename = filename+temp_postfix
            
        data_count = 0
        with open(_filename, "wb") as file:
            for data in response.iter_content(chunk_size=chunk_size):
                file.write(data)
                data_count = data_count + len(data)
                #yield data_count,content_size
        if temp_postfix:         
            shutil.move(_filename, filename)
        return data_count == content_size


def download_file_ex(url, path, filename_pre="", filename="", temp_postfix=".temp", download_timeout=600, replace=False, replace_compress=[".gz"], resume=True, *args, **kwargs):
    """
    带有超时检测的下载
    支持断点续传
    """
    connect_timeout = 10    
    if not filename:
        _response = requests.get(url, stream=True, timeout=(10, 10))
        _response.close()
        if not _response.ok:
            raise Exception(_response.text)
        filename = _response.headers["Content-Disposition"].split("filename=\"")[-1].split("\"")[0]
        filename = filename.encode("ISO-8859-1").decode("utf-8")            # header默认用ISO-8859-1编码
        filename = filename_pre+filename
    
    filename = os.path.join(path, filename)
    _filename = filename+temp_postfix
    
    if not replace: 
        if os.path.isfile(filename):
            # 文件已经存在的情况下不再重新下载
            return 1
        elif replace_compress:
            # 压缩文件也过滤
            for p in replace_compress:
                if os.path.isfile(filename+p):
                    return 1

    if path:
        os.makedirs(path, exist_ok=True)
    
    if resume and os.path.isfile(_filename):
        # 断点续传
        offset = os.path.getsize(_filename)
        open_mode = "ab"
    else:
        offset = 0
        open_mode = "wb"
    
    headers = {"Range":"bytes=%d-" % offset}
    
    with closing(requests.get(url, stream=True, headers=headers, timeout=(connect_timeout, download_timeout))) as response:
        if not response.ok:
            # 根据返回的状态码判断请求失败
            raise Exception(response.text)
        
        chunk_size = 1024                                                       # 单次持久化最大值
        content_size = int(response.headers["content-length"])                  # 内容体总大小
        data_count = 0    
        
        with open(_filename, open_mode) as file:
            for data in response.iter_content(chunk_size=chunk_size):
                file.write(data)
                data_count = data_count + len(data)
        
        if data_count != content_size:
            raise Exception("download length not match %s %s %s %s" % (url,filename,data_count,content_size))
        
        if temp_postfix:
            shutil.move(_filename, filename)
        
        return 1

            
def date_add(old_date, add_day):
    dt = datetime.datetime.strptime(old_date, "%Y-%m-%d")
    return (dt + datetime.timedelta(days= add_day )).strftime("%Y-%m-%d")


def _get_max_filename(dir):
    """
    获取该目录下最大的文件名，文件格式一致，如最后都以数字结尾
    """
    max_filename=""
    for dirpath, dirs, files in os.walk(dir):   
        for file in files:
            if max_filename<file:
                max_filename=file
   
    return max_filename     


def get_max_filename(dir, postfix="", force_postfix=0, cut_postfix=1, prefix="", force_prefix=0, cut_prefix=1):
    """
    获取该目录下最大的文件名，文件格式一致，如最后都以数字结尾
    包含前缀以及后缀的处理
    postfix           后缀的正则表达式
    force_postfix     是否必须存在后缀，即满足的才处理
    cut_postfix       返回值是否删除后缀
    """
    max_filename=("","")
    for dirpath, dirs, files in os.walk(dir):   
        for file in files:
            _file = file
            if re.match(".*"+postfix+"$", _file):
                _file = re.sub(postfix+"$", "", _file, 1)    
            elif force_postfix:
                # 强制存在后缀，不满足则跳过
                continue
        
            if re.match(prefix+".*", _file):
                _file = re.sub(prefix, "", _file, 1)       
            elif force_prefix:
                # 强制存在前缀，不满足则跳过
                continue
            
            if max_filename[1]<_file:
                max_filename = (file, _file)
    
    _max_filename = max_filename[0]
    if cut_postfix:
        _max_filename = re.sub(postfix+"$", "", _max_filename, 1)
    
    if cut_prefix:
        _max_filename = re.sub(prefix, "", _max_filename, 1)
     
    return _max_filename 


def iso_format_time(timestamp, time_gap=0,timezone="Asia/Shanghai"):
    """时间格式化 Timestamp ISO8601"""
    dt=datetime.datetime.fromtimestamp(timestamp+time_gap,pytz.timezone(timezone))
    x=dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return x[:-2]+":"+x[-2:]            
    
    
def avg(x,n=3):
    """数组的平均值"""
    return round(sum(x)/len(x),n)
