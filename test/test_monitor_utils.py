#!/bin/env python3
# -*- coding: utf-8 -*-
#
import os
import sys
import time

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config
from libs.tc import MonitorOpt
from libs.utils import iso_format_time


if __name__ == "__main__":
    
    id  = config.id
    key = config.key
    
    mo = MonitorOpt(id,key,"ap-shanghai")
    
    def mysql_m():
        instances = [{"Dimensions": [{"Name":"InstanceId","Value":"cdbro-xxxjc1be"}]},
            #{"Dimensions": [{"Name":"InstanceId","Value":"cdb-xxxs2utm"}]}
            ]
        
        metricname = "VolumeRate"
        now = time.time()
        starttime = iso_format_time(now,-1*86400)
        endtime = iso_format_time(now)
        
        period = 300
        namespace = "QCE/CDB"
        
        
        resp = mo.get_monitor_data(instances, metricname, starttime, endtime, period, namespace)
        
        print(resp.to_json_string()) 

    
    def redis_m():
        instances = [{"Dimensions": [{"Name":"instanceid","Value":"crs-48qoss39"},]},
            ]
        
        metricname = "MemUtil"
        now = time.time()
        starttime = iso_format_time(now,-1*86400)
        endtime = iso_format_time(now)
        
        period = 300
        namespace = "QCE/REDIS_MEM"
        
        
        resp = mo.get_monitor_data(instances, metricname, starttime, endtime, period, namespace)
        
        print(resp.to_json_string()) 
    
    
    def mongodb_m():
        instances = [{"Dimensions": [{"Name":"target","Value":"cmgo-li11kwml"},]},
            ]
        metricname = "ClusterDiskusage"
        now = time.time()
        #starttime = iso_format_time(now,-1*86400)
        starttime = iso_format_time(now)
        endtime = iso_format_time(now)
        
        period = 300
        namespace = "QCE/CMONGO"
        
        
        resp = mo.get_monitor_data(instances, metricname, starttime, endtime, period, namespace)
        
        print(resp.to_json_string()) 
    
    
    #mysql_m()
    #redis_m()
    mongodb_m()