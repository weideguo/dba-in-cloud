#!/bin/env python3
# -*- coding: utf-8 -*-
#
# 用于从腾讯云下载binlog备份，持续后台运行
# 重启时请删除备份目录下.temp结尾的文件，否则只备份在此之后的文件
# weideguo@dba 20210712
#
import os
import sys
import time
import shutil
#from imp import reload
from importlib import reload
from threading import Thread
from multiprocessing import Queue,Process
from traceback import format_exc

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from libs.myconcurrent import share_queue,retry_queue,ProcessControlThread
from libs.tc import CdbOpt as DbOpt
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
retry_queue = retry_queue

do = DbOpt(config.id, config.key)
do.region_list=config.region_list

def set_download_queue():
    """生产者"""
    max_files={}        #存放每个实例的最大binlog文件名，意味此文件之后的才需要重新获取下载url
    
    while True:

        try:
            all_instances = do.get_all_instances_info(subnet_ids=config.mysql_subnetid)          #大概每一天重新获取一次实例列表
        except:
            #获取实例信息错误，休眠一段时间后再次尝试，获取成功再进行下一步操作
            logger_err.warning(format_exc())
            time.sleep(60)
            continue
            
        for region,region_instance_list in all_instances:
            logger.info("instance for region %s are %s " % (region,[l.InstanceId for l in region_instance_list]))
        
        i = 0
        while i < 24:
            #每次运行时重新加载配置文件以实现热更改
            reload(config)
            #print(config.binlog_base_dir)
            for region,region_instance_list in all_instances:
                for l in region_instance_list:
                    
                    instance_id = l.InstanceId
                    instance_name = l.InstanceName
                    instance_type = l.InstanceType   # 1 主库 2 异地灾难备 3 从库
                    if instance_id not in config.binlog_exclude_instance and instance_type == 1:
                        path = os.path.join(config.binlog_base_dir, instance_name)
                        
                        max_filename = max_files.get(instance_id)
                        if not max_filename:
                            #第一次启动时以现存最大文件名开始，因此如果不想获取太之前的，则可以先在对应目录虚构一个文件
                            max_filename = get_max_filename(path, postfix="\.gz", prefix="\d{14}_\d{14}_", force_postfix=0, force_prefix=0, cut_postfix=1, cut_prefix=1)
                            max_files[instance_id] = max_filename
                        
                        # 防止堆积在下载队列导致url过期
                        while download_url_queue.qsize():
                            time.sleep(0.1)
                        
                        try:
                            url_list = do.get_binlogs_url_for_instance(region, instance_id, binlog_name_after=max_filename, max_try=100)
                        except:
                            # 获取单个实例的下载url错误，跳过，之后再次获取即可，从库没有备份
                            url_list=[]
                            logger_err.debug(format_exc())
                            logger_err.info("get binlog failed for %s" % {"region":region, "instance_id":instance_id, "binlog_name_after":max_filename})
                        
                        logger.info("new download for %s : %s" % (instance_id,[u.Name for u in url_list]))
                        
                        for u in url_list:
                            #IntranetUrl 内网 
                            #InternetUrl 外网
                            #请确保网络可通
                            if max_files[instance_id] < u.Name:
                                max_files[instance_id] = u.Name
                            download_url = u.IntranetUrl
                            filename = "%s_%s_%s" % (u.BinlogStartTime.replace(" ","").replace(":","").replace("-",""), u.BinlogFinishTime.replace(" ","").replace(":","").replace("-",""), u.Name)
                            binlog_name = u.Name
                            download_url_queue.put({"url":download_url, "path":path, "filename":filename,\
                                                    "region":region, "instance_id":instance_id, "binlog_name":binlog_name })
            
            logger.debug("max files in download queue are %s " % max_files)
            # 多少间隔获取一次binlog备份信息 可以适当修改
            time.sleep(10*60)
            i += 1


def download_retry():
    """重试操作"""
    while True:
        retry_info = retry_queue.get(block=True)
        
        # 防止堆积在下载队列导致url过期
        while download_url_queue.qsize():
            time.sleep(0.1)   
        
        region = retry_info["region"]
        instance_id = retry_info["instance_id"] 
        binlog_name = retry_info["binlog_name"]
        path = retry_info["path"]
        
        try:
            url_list = do.get_binlogs_url_for_instance(region, instance_id, binlog_name=binlog_name, max_try=100)
        except:
            retry_queue.put(retry_info)
            logger_err.warning(format_exc())
            logge_err.debug("retry get binlog faild, will retry %s " % retry_info)
            continue
        
        logger.info("retry download for %s : %s" % (instance_id,[format_filename(u.InternalAddr) for u in url_list]))
        
        for u in url_list:
            download_url = u.IntranetUrl
            filename = "%s_%s_%s" % (u.BinlogStartTime.replace(" ","").replace(":","").replace("-",""), u.BinlogFinishTime.replace(" ","").replace(":","").replace("-",""), u.Name)
            download_url_queue.put({"url":download_url, "path":path, "filename":filename,\
                                    "region":region, "instance_id":instance_id, "binlog_name":binlog_name })
    
        
def my_download(url, filename="", path=""):
    """消费者"""
    time.sleep(30)
    logger.info(url+" "+filename+" "+path)
   

if __name__ == "__main__":
    t1 = Thread(target = set_download_queue, args = ())    
    t2 = Thread(target = download_retry, args = ())    
    t1.start()
    t2.start()
    
    pct=ProcessControlThread(download_file_ex, config.binlog_process_num, config.binlog_thread_num, arg_type="dict", is_join=False)
    pct.start()
    
    t1.join()
    t2.join()
    pct.join()
    
