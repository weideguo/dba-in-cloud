#!/bin/env python3
# -*- coding: utf-8 -*-
#
# 数据库存储容量收集
# weideguo@dba 20220119
#

import os
import sys
import time
import json
import pytz
from traceback import format_exc
from datetime import datetime,timedelta

from pymongo import MongoClient 

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config
from libs.tc import CdbOpt,MonitorOpt,MongodbOpt
from libs.utils import iso_format_time
from libs.wrapper import logger, logger_err



stats_info = {}

now = time.time()
PERIOD = 300  
# 只要一组监控数据，即最新的数据可以
STARTTIME, ENDTIME = iso_format_time(now,-PERIOD),  iso_format_time(now,0)

instance_num = 10 

table_name = "volume_collect"

date_str = datetime.now(pytz.timezone("Asia/Shanghai")).strftime('%Y-%m-%d')

def get_mysql_volume(metricname="RealCapacity", namespace="QCE/CDB"):                      
    co = CdbOpt(config.id,config.key)
    co.region_list=config.region_list
    
    all_instances = co.get_all_instances_info(subnet_ids=[])
    
    for region,region_instance_list in all_instances:
        mo = MonitorOpt(config.id, config.key, region)
        instances=[]
        for l in region_instance_list:
            instance_id = l.InstanceId       
            stats_info[instance_id]={"InstanceName":l.InstanceName, "DbType":"mysql"}
                
            instances.append({"Dimensions": [{"Name":"InstanceId","Value": instance_id}]})
            
            if len(instances)>=instance_num:    
                _get_monitor_data(mo, instances, metricname, namespace)
                instances=[]
                    
        if instances:
            _get_monitor_data(mo, instances, metricname, namespace)
        
  
def _get_monitor_data(mo, instances, metricname, namespace):
    try:
        r = mo.get_monitor_data(instances, metricname, STARTTIME, ENDTIME, PERIOD, namespace)
        md = json.loads(r.to_json_string())
        for m in md["DataPoints"]:
            instance_id = m["Dimensions"][-1]["Value"]
            stats_info[instance_id][metricname] = m["Values"][-1]
    except:
        logger_err.error(format_exc())


def get_mongodb_volume(metricname="ClusterDiskusage", namespace="QCE/CMONGO"): 
    co = MongodbOpt(config.id,config.key)
    co.region_list=config.region_list
    
    all_instances = co.get_all_instances_info(project_ids=[])
    
    for region,region_instance_list in all_instances:
        mo = MonitorOpt(config.id, config.key, region)
        instances=[]
        for l in region_instance_list:
            instance_id = l.InstanceId       
            stats_info[instance_id]={"InstanceName":l.InstanceName, "DbType":"mongodb"}
                
            instances.append({"Dimensions": [{"Name":"target","Value": instance_id}]})
            
            if len(instances)>=instance_num:    
                _get_monitor_data(mo, instances, metricname, namespace)
                instances=[]
                    
        if instances:
            _get_monitor_data(mo, instances, metricname, namespace)

def stats_save():
    conn = MongoClient(config.mongodb_config["uri"])
    db = conn[config.mongodb_config["db"]]
    for instance_id in stats_info:
        try:
            #db[table_name].insert_one()
            db[table_name].update_one({"InstanceId":instance_id,"Date":date_str},{"$set":{**(stats_info[instance_id]),**{"Date":date_str}}},True)
        except:
            logger_err.error(format_exc())


if __name__ == "__main__":
    get_mysql_volume()
    get_mongodb_volume()
    stats_save()

