#!/bin/env python3
# -*- coding: utf-8 -*-
#
# 用于从腾讯云获取redis的负载并进行聚合统计，分析是否需要进行升/降
# weideguo@dba 20220110
#

import os
import sys
import time
import json
import copy
from traceback import format_exc
from threading import Thread,Lock
from queue import Queue

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config
from config import config_stats_redis as config_stats
from libs.tc import RedisOpt
from libs.tc import MonitorOpt
from libs.utils import iso_format_time,avg
from libs.wrapper import logger, logger_err
from libs.myconcurrent import FrequencyQueue


#分析的维度 参考api接口
#https://cloud.tencent.com/document/product/248/49729
metricnames = ["MemUtil"]

instance_num = 10      #单次请求多少个实例数的监控数据 接口上限是10个，但多个可能导致超过单请求的数据点数限制
thread_num   = 13      #请求监控数据的并发数  接口上限20次/s，接口的计数有一些偏差？  #https://cloud.tencent.com/document/api/248/31014
output_path  = "/tmp"  #输出文件的目录

#######################################################################################
lock = Lock()

#接口的访问频率限制由此控制
#实际获取监控信息并发控制，带频率限制                                                                                         
get_monitor_queue = FrequencyQueue(thread_num, thread_num, time_space=1, error_logger=logger.debug)       

now = time.time()

stats_info = {}                               #存储统计与分析的结果
time_pair=[]

#考虑到每个时间段的粒度可能不一样，因此每个时间段单独获取 请参考接口控制获取的粒度
time_pair.append(("24h",iso_format_time(now,-86400),   iso_format_time(now,0),3600))   #24h 
time_pair.append(("7d" ,iso_format_time(now,-7*86400), iso_format_time(now,0),3600))   #7d  
time_pair.append(("30d",iso_format_time(now,-30*86400),iso_format_time(now,0),3600))   #30d 

type_avg="_avg_"
type_max="_max_"

mem_fields =[metricnames[0].lower()+type_avg+x[0] for x in time_pair] 
mem_fields +=[metricnames[0].lower()+type_max+x[0] for x in time_pair] 

#输出的指标
fields = mem_fields

def get_monitor_data(instance_num=3):
    """
    获取实例的监控数据
    instance_num 多少个实例请求一次 小于10 过多的实例可能导致数据获取不全 
    """
    co = RedisOpt(config.id,config.key)
    co.region_list=config.region_list

    all_instances = co.get_all_instances_info()
    
    for region,region_instance_list in all_instances:
        mo = MonitorOpt(config.id, config.key, region)
        instances=[]
        for l in region_instance_list:
            instance_id = l.InstanceId       
            stats_info[instance_id]={"instance_name":l.InstanceName,"vip":l.WanIp,"create_time":l.Createtime,"size":l.Size, "metrics":{}}
                 
            instances.append({"Dimensions": [{"Name":"instanceid","Value": instance_id}]})
            
            if len(instances)>=instance_num:    
                _get_monitor_data(mo, instances, time_pair)
                instances=[]
                      
        if instances:
            _get_monitor_data(mo, instances, time_pair)

    #等待所有线程执行结束 即获取完所有监控数据
    get_monitor_queue.join()


def _get_monitor_data(mo, instances, time_pair):
    """
    每一组实例的监控数据请求集合
    """
    for metricname in metricnames: 
        for time_type, starttime, endtime,period in time_pair:
            field_avg = metricname.lower()+type_avg+time_type
            field_max = metricname.lower()+type_max+time_type
            
            get_monitor_queue.put(1)      #控制线程并发数  实现一个带频率限制的队列
            t=Thread(target=__get_monitor_data,args=(mo, instances, metricname, starttime, endtime, period, field_avg, field_max))
            t.start()
                

def __get_monitor_data(mo, instances, metricname, starttime, endtime, period, field_avg, field_max):
    """
    单个并发的实际操作 调用接口获取监控数据
    """
    global get_monitor_queue
    
    try:
        r = mo.get_monitor_data(instances, metricname, starttime, endtime, period, namespace = "QCE/REDIS_MEM")
        md = json.loads(r.to_json_string())
        
        with lock:
            for m in md["DataPoints"]:
                instance_id = m["Dimensions"][-1]["Value"]
                stats_info[instance_id]["metrics"][field_avg] = avg(m["Values"]) if m["Values"] else -1
                stats_info[instance_id]["metrics"][field_max] = max(m["Values"]) if m["Values"] else -1
        
    except:
        with lock:
            #调用接口出错时虚构数据占位
            for instance in instances:
                instance_id = instance["Dimensions"][-1]["Value"]
                stats_info[instance_id]["metrics"][field_avg] = -1
                stats_info[instance_id]["metrics"][field_max] = -1 
        
        logger_err.error(format_exc())
        logger_err.error("get monitor data failed for %s " % [instances, metricname, starttime, endtime, period])   

    finally:
        get_monitor_queue.get()
        get_monitor_queue.task_done()
    

def analyze_monitor_data():
    """
    0   保持不变
    1   降
    2   考虑降
    -1  升
    -2  考虑升
    3   内存考虑降 如果可以只降内存的情况
    100 异常 需要分析
    """
    #print(stats_info)    
    for instance_id in stats_info.keys():
        instance_metrics = stats_info[instance_id]["metrics"]
        
        #
        if all([ instance_metrics[field]   <= config_stats.redis_mem_levels[0] for field in mem_fields ]):
            #低
            disk_analyze = 1
        elif all([ instance_metrics[field] <= config_stats.redis_mem_levels[1] for field in mem_fields ]):
            #次低
            disk_analyze = 2
        elif any([ instance_metrics[field] >= config_stats.redis_mem_levels[3] for field in mem_fields ]):
            #高
            disk_analyze = -1            
        elif any([ instance_metrics[field] >= config_stats.redis_mem_levels[2] for field in mem_fields ]):
            #次高
            disk_analyze = -2   
        else:
            #保持不变
            disk_analyze = 0
        
        stats_info[instance_id]["mem_analyze"] = disk_analyze
      
              
def output_csv(filename):
    """
    将数据输出成csv文件
    """
    with open(filename,"w") as f:
        instance_info_fields=["instance_name","vip","create_time","size","mem_analyze"]
        f.write("instance_id,"+",".join(instance_info_fields)+","+",".join(fields)+"\r\n")
        for instance_id in stats_info.keys():
            instance_info =",".join([ str(stats_info[instance_id][instance_info_field]) for instance_info_field in instance_info_fields ])
            f.write(instance_id+","+instance_info+","+",".join([str(stats_info[instance_id]["metrics"][field]) for field in fields])+"\r\n")
        

if __name__ == "__main__":
    get_monitor_data(instance_num)
    analyze_monitor_data()
    
    filename = "redis_stats_%s.csv" % time.strftime('%Y%m%d_%H%M%S',time.localtime())  
    full_filename = os.path.join(output_path,filename)
    output_csv(full_filename)
    print(full_filename)

