#!/bin/env python3
# -*- coding: utf-8 -*-
#
# 用于从腾讯云获取mysql的负载并进行聚合统计，分析是否需要进行升/降
# weideguo@dba 20210713
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

from config import config,config_stats
from libs.tc import CdbOpt
from libs.tc import MonitorOpt
from libs.utils import iso_format_time,avg
from libs.wrapper import logger, logger_err
from libs.myconcurrent import FrequencyQueue


#分析的维度 参考api接口
#https://cloud.tencent.com/document/product/248/45147
metricnames = ["VolumeRate", "CpuUseRate", "MemoryUseRate", "Qps", "Tps"]

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
time_pair.append(("24h",iso_format_time(now,-86400),   iso_format_time(now,0),300))   #24h 
time_pair.append(("7d" ,iso_format_time(now,-7*86400), iso_format_time(now,0),300))   #7d  
time_pair.append(("30d",iso_format_time(now,-30*86400),iso_format_time(now,0),300))   #30d 

type_avg="_avg_"
type_max="_max_"

disk_fields    =[metricnames[0].lower()+type_avg+x[0] for x in time_pair] 
disk_fields   +=[metricnames[0].lower()+type_max+x[0] for x in time_pair] 

cpu_avg_fields =[metricnames[1].lower()+type_avg+x[0] for x in time_pair] 
cpu_max_fields =[metricnames[1].lower()+type_max+x[0] for x in time_pair] 

mem_avg_fields =[metricnames[2].lower()+type_avg+x[0] for x in time_pair] 
mem_max_fields =[metricnames[2].lower()+type_max+x[0] for x in time_pair] 

qps_avg_fields =[metricnames[3].lower()+type_avg+x[0] for x in time_pair] 
qps_max_fields =[metricnames[3].lower()+type_max+x[0] for x in time_pair]

tps_avg_fields =[metricnames[4].lower()+type_avg+x[0] for x in time_pair] 
tps_max_fields =[metricnames[4].lower()+type_max+x[0] for x in time_pair] 

#输出的指标
fields = disk_fields + cpu_avg_fields+ cpu_max_fields +\
         mem_avg_fields+ mem_max_fields + \
         qps_avg_fields+ qps_max_fields + \
         tps_avg_fields+ tps_max_fields

       
def get_monitor_data(instance_num=3):
    """
    获取实例的监控数据
    instance_num 多少个实例请求一次 小于10 过多的实例可能导致数据获取不全 
    """
    co = CdbOpt(config.id,config.key)
    co.region_list=config.region_list

    all_instances = co.get_all_instances_info(subnet_ids=config.mysql_subnetid)
    
    for region,region_instance_list in all_instances:
        mo = MonitorOpt(config.id, config.key, region)
        instances=[]
        for l in region_instance_list:
            instance_id = l.InstanceId       
            stats_info[instance_id]={"instance_name":l.InstanceName,"vip":l.Vip,"create_time":l.CreateTime,"volume":l.Volume,"instance_type": "%sC%sM" % (l.Cpu,l.Memory), "metrics":{}}
                 
            instances.append({"Dimensions": [{"Name":"InstanceId","Value": instance_id}]})
            
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
        r = mo.get_monitor_data(instances, metricname, starttime, endtime, period)
        md = json.loads(r.to_json_string())
        
        with lock:
            for m in md["DataPoints"]:
                instance_id = m["Dimensions"][-1]["Value"]
                stats_info[instance_id]["metrics"][field_avg] = avg(m["Values"])
                stats_info[instance_id]["metrics"][field_max] = max(m["Values"])    
        
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
    
    #为qps/tps百分比设置新的属性
    tps_max_fields_pair = [("_"+x,x) for x in tps_max_fields]
    qps_max_fields_pair = [("_"+x,x) for x in qps_max_fields]
    
    tps_avg_fields_pair = [("_"+x,x) for x in tps_avg_fields]
    qps_avg_fields_pair = [("_"+x,x) for x in qps_avg_fields]
    
    
    _stats_info  = copy.deepcopy(stats_info)    
    #计算相对于参考的qps/tps的百分比
    for instance_id in _stats_info.keys():
        
        #如果不存在，虚构一个qps/tps参考值，但计算结果会异常，因此要确保对应配置的参考值存在
        qps_tps_pair = config_stats.performance_ref.get(_stats_info[instance_id]["instance_type"],(1,1))
        stats_info[instance_id]["qps/tps"] = "/".join([ str(x) for x in qps_tps_pair])
        
        for field_new,field in qps_max_fields_pair+qps_avg_fields_pair:
            _stats_info[instance_id]["metrics"][field_new] = round(100 * _stats_info[instance_id]["metrics"][field] / qps_tps_pair[0],3)
        
        for field_new,field in tps_max_fields_pair+tps_avg_fields_pair:
            _stats_info[instance_id]["metrics"][field_new] = round(100 * _stats_info[instance_id]["metrics"][field] / qps_tps_pair[1],3)
        
    
    for instance_id in stats_info.keys():
        instance_metrics = _stats_info[instance_id]["metrics"]
        
        #磁盘空间 分档 mysql_disk_levels=[15,30,80,90]
        if all([ instance_metrics[field]   <= config_stats.mysql_disk_levels[0] for field in disk_fields ]):
            #低
            disk_analyze = 1
        elif all([ instance_metrics[field] <= config_stats.mysql_disk_levels[1] for field in disk_fields ]):
            #次低
            disk_analyze = 2
        elif any([ instance_metrics[field] >= config_stats.mysql_disk_levels[3] for field in disk_fields  ]):
            #高
            disk_analyze = -1            
        elif any([ instance_metrics[field] >= config_stats.mysql_disk_levels[2] for field in disk_fields  ]):
            #次高
            disk_analyze = -2   
        else:
            #保持不变
            disk_analyze = 0
            
        #cpu与内存 
        if all([ instance_metrics[field] <= config_stats.mysql_cpu_levels[0][0] for field   in cpu_avg_fields ])      and \
           all([ instance_metrics[field] <= config_stats.mysql_qps_levels[0][0] for field,_ in qps_avg_fields_pair ]) and \
           all([ instance_metrics[field] <= config_stats.mysql_tps_levels[0][0] for field,_ in tps_avg_fields_pair ]) and \
           all([ instance_metrics[field] <= config_stats.mysql_cpu_levels[0][1] for field   in cpu_max_fields ])      and \
           all([ instance_metrics[field] <= config_stats.mysql_qps_levels[0][1] for field,_ in qps_max_fields_pair ]) and \
           all([ instance_metrics[field] <= config_stats.mysql_tps_levels[0][1] for field,_ in tps_max_fields_pair ]):
            #低负载
            cpu_memory_analyze = 1
           
        elif all([ instance_metrics[field] <= config_stats.mysql_cpu_levels[1][0] for field   in cpu_avg_fields ])      and \
             all([ instance_metrics[field] <= config_stats.mysql_qps_levels[1][0] for field,_ in qps_avg_fields_pair ]) and \
             all([ instance_metrics[field] <= config_stats.mysql_tps_levels[1][0] for field,_ in tps_avg_fields_pair ]) and \
             all([ instance_metrics[field] <= config_stats.mysql_cpu_levels[1][1] for field   in cpu_max_fields ])      and \
             all([ instance_metrics[field] <= config_stats.mysql_qps_levels[1][1] for field,_ in qps_max_fields_pair ]) and \
             all([ instance_metrics[field] <= config_stats.mysql_tps_levels[1][1] for field,_ in tps_max_fields_pair ]):
            #次低负载
            cpu_memory_analyze = 2
        
        elif any([ instance_metrics[field] >= config_stats.mysql_cpu_levels[3] for field   in cpu_avg_fields ])      or \
             any([ instance_metrics[field] >= config_stats.mysql_qps_levels[3] for field,_ in qps_avg_fields_pair ]) or \
             any([ instance_metrics[field] >= config_stats.mysql_tps_levels[3] for field,_ in tps_avg_fields_pair ]):
            #高负载 
            cpu_memory_analyze = -1

        elif any([ instance_metrics[field] >= config_stats.mysql_cpu_levels[2] for field   in cpu_avg_fields ])      or \
             any([ instance_metrics[field] >= config_stats.mysql_qps_levels[2] for field,_ in qps_avg_fields_pair ]) or \
             any([ instance_metrics[field] >= config_stats.mysql_tps_levels[2] for field,_ in tps_avg_fields_pair ]):
            #次高负载 
            cpu_memory_analyze = -2
        
        elif any([ instance_metrics[field] >= config_stats.mysql_cpu_levels[4] for field   in cpu_max_fields ])      or \
             any([ instance_metrics[field] >= config_stats.mysql_qps_levels[4] for field,_ in qps_max_fields_pair ]) or \
             any([ instance_metrics[field] >= config_stats.mysql_tps_levels[4] for field,_ in tps_max_fields_pair ]) or \
             any([ instance_metrics[field] >= config_stats.mysql_mem_levels[1] for field   in mem_max_fields ]):     
            #异常情况 cpu qps tps mem使用率超过100
            cpu_memory_analyze = 100
        
        elif all([ instance_metrics[field] <= config_stats.mysql_mem_levels[0]  for field in mem_avg_fields ]):
            #低内存判断需要在没有其他情况后再考虑
            cpu_memory_analyze = 3
        
        else:
            #都不满足则维持不变
            cpu_memory_analyze = 0
    
        stats_info[instance_id]["disk_analyze"] = disk_analyze
        stats_info[instance_id]["cpu_memory_analyze"] = cpu_memory_analyze
        
              
def output_csv(filename):
    """
    将数据输出成csv文件
    """
    with open(filename,"w") as f:
        instance_info_fields=["instance_name","vip","create_time","instance_type","qps/tps","volume","cpu_memory_analyze","disk_analyze"]
        f.write("instance_id,"+",".join(instance_info_fields)+","+",".join(fields)+"\r\n")
        for instance_id in stats_info.keys():
            #instance_info =",".join([instance_id, stats_info[instance_id]["instance_name"],stats_info[instance_id]["instance_type"], str(stats_info[instance_id]["cpu_memory_analyze"]), str(stats_info[instance_id]["disk_analyze"]) ])
            instance_info =",".join([ str(stats_info[instance_id][instance_info_field]) for instance_info_field in instance_info_fields ])
            f.write(instance_id+","+instance_info+","+",".join([str(stats_info[instance_id]["metrics"][field]) for field in fields])+"\r\n")
        


if __name__ == "__main__":
    get_monitor_data(instance_num)
    analyze_monitor_data()
    
    filename = "mysql_stats_%s.csv" % time.strftime('%Y%m%d_%H%M%S',time.localtime())  
    full_filename = os.path.join(output_path,filename)
    output_csv(full_filename)
    print(full_filename)

