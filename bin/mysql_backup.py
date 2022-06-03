#!/bin/env python3
# -*- coding: utf-8 -*-
#
# 用于从腾讯云下载当天的备份，也可以传入任意时间的
# weideguo@dba 20210713
#
import os
import sys
import time
import datetime
import requests
import optparse
from contextlib import closing
from threading import Thread
from multiprocessing import Queue,Process
from traceback import format_exc
from queue import Empty

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from libs.myconcurrent import share_queue,retry_queue,ProcessControlThread
from libs.tc import CdbOpt as DbOpt
from libs.utils import download_file_ex,date_add
from libs.wrapper import logger, logger_err
from config import config

download_url_queue = share_queue     #下载url队列
retry_queue = retry_queue            #下载url调用失败的重试队列
        
do = DbOpt(config.id, config.key)
do.region_list=config.region_list        

pct = ProcessControlThread(download_file_ex, config.backup_process_num, config.backup_thread_num, arg_type="dict", is_join=False, is_retry=True)
        
def get_instance_info(filter_date, instance, filename, max_try=5):
    
    i=0
    while i<max_try:
        i += 1
        try:
            all_instances = do.get_all_instances_info(subnet_ids=config.mysql_subnetid)          
            break
        except:
            #获取实例信息错误，休眠一段时间后再次尝试
            logger_err.warning(format_exc())
            
            if i >= max_try:
                logger_err.error("get instances info failed")
                exit(1)
            else:
                time.sleep(60)
    
    for region, region_instance_list in all_instances:    
        for l in region_instance_list:
            if instance and instance not in [l.InstanceId,l.InstanceName]:
                #print(l.InstanceId, l.InstanceName, instance)
                continue
                
            instance_id = l.InstanceId
            instance_type = l.InstanceType   # 1 主库 2 异地灾难备 3 从库
            if instance_id not in config.backup_exclude_instance and instance_type == 1:
                download_url = None
                init_info = {"url":download_url, "path":config.backup_dir, "filename":filename,\
                             "region":region, "instance_id":instance_id, "filter_date":filter_date}
                               
                retry_queue.put(init_info)
                
                
def set_download_queue(timeout = 600): 
    """
    设置下载队列
    """   
    while True:
        try:
            retry_info = retry_queue.get(block=True, timeout=timeout)
            filename    = retry_info["filename"]
            region      = retry_info["region"]
            instance_id = retry_info["instance_id"]
            filter_date = retry_info["filter_date"]
            
            # 防止堆积在下载队列导致url过期
            while download_url_queue.qsize():
                time.sleep(0.1)
            
            try:
                download_info = do.get_backup_url_for_instance(region, instance_id, filter_date)
                download_url = download_info.IntranetUrl
            except:
                logger_err.error(format_exc())
                logger_err.error("get download url for instance [ %s %s ] failed, will retry" % (instance_id, filter_date))
                retry_queue.put(retry_info)
                continue
            
            if download_url:
                retry_info["url"] = download_url
                download_url_queue.put(retry_info)    
            else:
                # 只读实例没有备份
                logger.warning("get download url for instance [ %s %s ] is null" % (instance_id, filter_date))
        
        except Empty:
            if pct.tsize():
                # 后台并发还存在，因此继续
                logger.debug("get empty retry queue, but wait for background concurrent")
                continue
            else:
                # 队列为空，认为没有重试的任务，因此直接结束即可
                logger.debug("get retry queue timeout, seems good to end")
                break 
                
        except:
            logger_err.error(format_exc())
            break
    
    # 设置结束符
    for i in range(config.backup_process_num): 
        download_url_queue.put("EOF")          
    

def main(backup_date, instance, filename):    
    get_instance_info(backup_date, instance,filename)         
    
    t = Thread(target = set_download_queue, args = ())
    t.start()
    
    pct.start()

    t.join()
    pct.join()


def arg_parse():
    """
    命令行参数解析
    """
    usage = "Usage: %prog [options]"
    usage += "\n\nstart with following options:"
    
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-b", "--backup_date", default=time.strftime("%Y-%m-%d",time.localtime()), help="下载备份的日期，默认当天，如 2021-01-01 或 lastest")
    parser.add_option("-i", "--instance", default="", help="只获取该实例的备份，可为实例id或者实例名，为空则下载所有的实例")
    parser.add_option("-f", "--filename", default="", help="保存的文件名，为空则使用下载时传过来的文件名")
    
    return parser.parse_args()    


if __name__ == "__main__":
    options, args = arg_parse()
    backup_date   = options.backup_date     
    instance      = options.instance 
    filename      = options.filename
    
    print("--------------- backup begin for %s in %s ------------------" % (backup_date, time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())))   

    main(backup_date, instance, filename)
    
    print("--------------- backup finish for %s in %s ------------------" % (backup_date, time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())))   

