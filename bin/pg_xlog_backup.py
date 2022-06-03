#!/bin/env python3
# -*- coding: utf-8 -*-
#
# 用于从腾讯云下载binlog备份，持续后台运行
# 重启时请删除备份目录下.temp结尾的文件，否则只备份在此之后的文件
# weideguo@dba 20220126
#
import re
import os
import sys
import time
import pytz
import shutil
from datetime import datetime
from importlib import reload
from threading import Thread
from multiprocessing import Queue,Process
from traceback import format_exc

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from libs.myconcurrent import share_queue,retry_queue,ProcessControlThread
from libs.tc import PgOpt as DbOpt
from libs.tc.pg_utils import format_filename
from libs.utils import get_max_filename,download_file_ex
from libs.wrapper import logger, logger_err
from config import config

"""
CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0
"""
logger.setLevel(10)
logger_err.setLevel(10)



download_url_queue = share_queue 
retry_queue = retry_queue            #下载url调用失败的重试队列

tz = pytz.timezone("Asia/Shanghai")

do = DbOpt(config.id, config.key)
do.region_list = config.region_list


def set_download_queue():
    """生产者"""
    max_files = {}        #存放每个实例的最大binlog文件名，意味此文件之后的才需要重新获取下载url
    last_timestamp = None
    while True:
        try:
            all_instances = do.get_all_instances_info()          #大概每一天重新获取一次实例列表
        except:
            #获取实例信息错误，休眠一段时间后再次尝试，获取成功再进行下一步操作
            logger_err.warning(format_exc())
            time.sleep(60)
            continue
            
        for region,region_instance_list in all_instances:
            logger.info("instance for region %s are %s " % (region,[l.DBInstanceId for l in region_instance_list]))
        
        i = 0
        while i < 24:
            #每次运行时重新加载配置文件以实现热更改
            reload(config)
            now_timestamp = time.time()
            if not last_timestamp:
                # 第一次启动时获取前一天的
                last_timestamp = now_timestamp - 86400
            
            binlog_time_after = datetime.fromtimestamp(last_timestamp-1800,tz).strftime("%Y-%m-%d %H:%M:%S") 
            binlog_time_max   = datetime.fromtimestamp(now_timestamp,tz).strftime("%Y-%m-%d %H:%M:%S") 
            last_timestamp = now_timestamp
            for region,region_instance_list in all_instances:
                for l in region_instance_list:
                    
                    instance_id = l.DBInstanceId
                    instance_name = l.DBInstanceName
                    if instance_id not in config.binlog_exclude_instance:
                        path = os.path.join(config.pg_binlog_base_dir, instance_name)
                        
                        max_filename = max_files.get(instance_id)
                        if not max_filename:
                            #第一次启动时以现存最大文件名开始，因此如果不想获取太之前的，则可以先在对应目录虚构一个文件
                            max_filename = get_max_filename(path, postfix="\.gz", prefix="", force_postfix=0, force_prefix=0, cut_postfix=1, cut_prefix=1)
                            max_files[instance_id] = max_filename
                        
                        # 防止堆积在下载队列导致url过期
                        while download_url_queue.qsize():
                            time.sleep(0.1)
                        
                        try:
                            url_list = do.get_xlogs_url_for_instance(region, instance_id, binlog_time_after=binlog_time_after, binlog_time_max=binlog_time_max, max_try=100)
                        except:
                            #获取单个实例的下载url错误，跳过，之后再次获取即可
                            url_list=[]
                            logger_err.warning(format_exc())
                        
                        if len(url_list):
                            logger.info("new download for %s : %s" % (instance_id,[format_filename(u.InternalAddr) for u in url_list]))
                        
                        # url_list 应该逆序
                        for u in url_list:
                            #InternalAddr 内网 
                            #ExternalAddr 外网
                            #请确保网络可通   
                            download_url = u.InternalAddr
                            filename = format_filename(u.InternalAddr)
                            if filename <= max_files[instance_id]:
                                # 新下载的文件名等于已经下载的最大文件名，即表示该实例此次获取的下载队列不用再下载
                                break
                            init_info = {"url":download_url, "path":path, "filename":filename, \
                                         "region":region, "instance_id":instance_id, \
                                         "binlog_time_after":binlog_time_after, "binlog_time_max":binlog_time_max, "max_try":100}
                            download_url_queue.put(init_info)
                        max_files[instance_id] = format_filename(url_list[0].InternalAddr) if url_list else max_files[instance_id]
            
            logger.debug("get xlog from %s to %s " % (binlog_time_after,binlog_time_max))
            logger.debug("max files in download queue are %s " % max_files)
            # 多少间隔获取一次binlog备份信息 可以适当修改
            time.sleep(10*60)
            i += 1


def download_retry(): 
    while True:
        retry_info = retry_queue.get(block=True)
        instance_id = retry_info["instance_id"] 
        
        # 防止堆积在下载队列导致url过期
        while download_url_queue.qsize():
            time.sleep(0.1)
        
        try:
            url_list = do.get_xlogs_url_for_instance(**retry_info)
        except:
            #获取单个实例的下载url错误，跳过，之后再次获取即可
            url_list=[]
            retry_queue.put(retry_info)
            logger_err.warning(format_exc())
            logge_err.debug("get_xlogs_url_for_instance faild %s" % retry_info)
        
        if len(url_list):
            logger.info("new download for %s : %s" % (instance_id,[format_filename(u.InternalAddr) for u in url_list]))
        
        # url_list 应该逆序
        for u in url_list:
            #InternalAddr 内网 
            #ExternalAddr 外网
            #请确保网络可通   
            try:
                retry_info["url"] = u.InternalAddr
                retry_info["filename"] = format_filename(u.InternalAddr)
                download_url_queue.put(retry_info)
                logger.debug("put download queue %s" % retry_info)
            except:
                logger_err.error(format_exc())
                logger_err.error("get download url failed for %s" % u)
                

def my_download(url, filename="", path="", *args, **kwargs):
    """消费者"""
    time.sleep(30)
    logger.info(url+" "+filename+" "+path)
   

if __name__ == "__main__":
    t1 = Thread(target = set_download_queue, args = ())    
    t2 = Thread(target = download_retry, args = ())    
    t1.start()
    t2.start()
    
    pct = ProcessControlThread(download_file_ex, config.xlog_process_num, config.xlog_thread_num, arg_type="dict", is_join=False, is_retry=True)
    pct.start()
    
    t1.join()
    t2.join()
    pct.join()
    
