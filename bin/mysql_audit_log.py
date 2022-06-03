#!/bin/env python3
# -*- coding: utf-8 -*-
#
# 用于从腾讯云下载全审计的日志，对应实例应该先开启`数据库审计`功能
# weideguo@dba 20211130
#
#
import os
import sys
import time
import optparse

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from libs.tc import CdbOpt
from libs.utils import download_file
from config import config


RE_TRY_TIME = 60*60        # 等待审计日志生成时间，每次等1s
AUDIT_LOG_PATH = "/tmp"    # 审计日志存放位置



def main(instance, filter_ip, start_time, end_time):
    co = CdbOpt(config.id, config.key)
    co.region_list = config.region_list
    all_instances = co.get_all_instances_info(subnet_ids=config.mysql_subnetid)
    match_field   = ["InstanceId","InstanceName"]
    _instance_info = co.get_instance_info(instance, all_instances, match_field, match_len=1)
    if len(_instance_info) != 1:
        raise Exception("filter instance_info too long")
    
    instance_info=_instance_info[0]
    
    region     = instance_info.Region   
    instance_id = instance_info.InstanceId
    instance_name = instance_info.InstanceName
    
    filter = {"Host": filter_ip, "SqlTypes": [ "SELECT" ]}
    #filter = {"Host": filter_ip,  "SqlType": "SELECT"}     
    
    audit_log_name = co.create_audit_log(region, instance_id, start_time, end_time, filter)
    
    audit_log_info = None
    i=0
    while i<RE_TRY_TIME:
        audit_log_info = co.get_audit_log_url(region, instance_id, audit_log_name)
        url = audit_log_info.DownloadUrl
        if not url:
            time.sleep(1)
            i += 1
            print("try get download url %s" % i)
        else:
            break

    if not url:
        raise Exception("can not get url for audit file: %s" % audit_log_name)
        
    filename = "%s_%s.csv" % (instance_name, time.strftime("%Y%m%d_%H%M%S",time.localtime()) )

    print("now begin download")
    download_file(url=url, path=AUDIT_LOG_PATH, filename=filename)
    
    return os.path.join(AUDIT_LOG_PATH, filename)
    

def arg_parse():
    """
    命令行参数解析
    """
    usage = "Usage: %prog [options]"
    usage += "\n\nstart with following options:"
    
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-i", "--instance",   help=u"只获取该实例的备份，可为实例id或者实例名（必须）")
    parser.add_option("-s", "--start_time", help=u"开始时间 2021-12-02 00:00:00")
    parser.add_option("-e", "--end_time",   help=u"结束时间 2021-12-02 01:00:00")
    parser.add_option("-f", "--filter_ip", help=u"只过滤获取对应ip的操作日志，多个以”,“分隔")
    
    return parser.parse_args()    


if __name__ == "__main__":
    options, args = arg_parse()
    instance      = options.instance 
    start_time    = options.start_time
    end_time      = options.end_time
    filter_ip     = options.filter_ip
    
    if not instance or not start_time or not end_time or not filter_ip:
        raise Exception("must input enought vars")

    filter_ip = filter_ip.split(",")

    f = main(instance, filter_ip, start_time, end_time)
    print(f)
    print("get audit log for instance [ %s ] done" %  instance)
