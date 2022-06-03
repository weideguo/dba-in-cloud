#coding:utf8
# -*- coding: utf-8 -*-
#!/bin/env python3
#
# 用于从腾讯云下载binlog查询与下载
# weideguo@dba 20211213
#
import os
import sys
import optparse

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from libs.myconcurrent import share_queue,ProcessControlThread
from libs.tc import CdbOpt
from libs.utils import download_file_ex
from config import config

# binlog临时存放的目录
TMP_BINLOG_BASE_DIR="/tmp"

download_url_queue = share_queue 

def main(instance, start_datetime, stop_datetime, opt_type):
    co = CdbOpt(config.id, config.key)
    co.region_list = config.region_list
    all_instances = co.get_all_instances_info(subnet_ids=config.mysql_subnetid)
    match_field   = ["InstanceId","InstanceName","Vip"]
    _instance_info = co.get_instance_info(instance, all_instances, match_field, match_len=1)
    if len(_instance_info) != 1:
        raise Exception("filter instance_info too long")
    
    instance_info=_instance_info[0]
    
    _region     = instance_info.Region   
    instance_id = instance_info.InstanceId
    instance_name = instance_info.InstanceName
    
    url_list = co.get_binlogs_url_for_instance(_region, instance_id, binlog_time_after=start_datetime, binlog_time_max=stop_datetime, max_try=1000)
    
    if not url_list:
        raise Exception("null download info error")
            
    filenames = []
    for url_info in url_list:
        filename     = url_info.Name
        download_url = url_info.IntranetUrl
        
        #download_file_ex(url=download_url, path=TMP_BINLOG_BASE_DIR, filename=filename)
        download_url_queue.put({"url":download_url, "path":TMP_BINLOG_BASE_DIR, "filename":filename})
        
        filenames.append(filename)
        
    filenames.reverse()
    # print(filenames)
    print(TMP_BINLOG_BASE_DIR+":::"+" ".join(filenames)) 

    if opt_type != "query":
        pct=ProcessControlThread(download_file_ex, config.binlog_process_num, config.binlog_thread_num, arg_type="dict", is_join=False)
        pct.start()
        
        # 不需要一直后台运行，因此启动多少个进程，则往队列设置多少终止标识
        for i in range(config.binlog_process_num): 
            download_url_queue.put("EOF")
        
        pct.join()

        
def arg_parse():
    """
    命令行参数解析
    """
    usage = "Usage: %prog [options]"
    usage += "\n\nstart with following options:"
    
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-i", "--instance", help=u"只获取该实例的备份，可为实例id、实例名、ip")
    parser.add_option("-b", "--start-datetime",   help=u"开始时间")
    parser.add_option("-e", "--stop-datetime" ,   help=u"结束时间")
    parser.add_option("-o", "--opt-type", choices=["query","download"],       help=u"操作类型（query/download）")
    
    return parser.parse_args()    


if __name__ == "__main__":
    options, args  = arg_parse()
    instance       = options.instance
    start_datetime = options.start_datetime
    stop_datetime  = options.stop_datetime
    opt_type       = options.opt_type
    
    
    if None in [instance,start_datetime,stop_datetime,opt_type]:
        raise Exception("must input enought vars")

    main(instance, start_datetime, stop_datetime, opt_type)
