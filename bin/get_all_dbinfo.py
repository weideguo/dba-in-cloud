#!/bin/env python3
# -*- coding: utf-8 -*-
#
# 用于从腾讯云获取所有数据库的信息 mysql mongodb redis
# weideguo@dba 20210827
#

import os
import sys
import json
from traceback import format_exc

from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config
from libs.tc import CdbOpt
from libs.tc import MongodbOpt
from libs.tc import RedisOpt
from libs.wrapper import logger, logger_err


"""
InstanceName, InstanceId, Vip,   Vport   CDB
InstanceName, InstanceId, Vip,   Vport   mongodb
InstanceName, InstanceId, WanIp, Port    redis
"""

#从url获取的字段，如果跟存储于数据库的字段不一致，则需要指定
opt_list = [
("mysql"  ,CdbOpt,    ()),
("mongodb",MongodbOpt,()),
("redis"  ,RedisOpt,  ("InstanceName", "InstanceId", "WanIp", "Port"))
]

#存储于数据库的字段
default_fileds = ("InstanceName", "InstanceId", "Vip",   "Vport")


conn = MongoClient(config.mongodb_config["uri"])
db = conn[config.mongodb_config["db"]]

table_name = "db_info"                       # 已经不用的 InstanceId 会被保留，不插入的字段会被保留
full_table_name = "db_info_full"             # 存储当前使用的所有 InstanceId

db[full_table_name].delete_many({"Tag": config.tag})

for db_type,db_opt,fileds in opt_list:
    fileds = fileds or default_fileds
    
    do = db_opt(config.id, config.key)
    do.region_list = config.region_list
    instance_infos = do.get_all_instances_info()
    
    single_type_table_name = db_type+"_info"
    db[single_type_table_name].delete_many({"Tag": config.tag})
    
    for region,region_instance_infos in instance_infos:
        for region_instance_info in region_instance_infos:
            #print(db_type,region,json.loads(region_instance_info.to_json_string()))
            instance_info = json.loads(region_instance_info.to_json_string())
            db[single_type_table_name].insert_one({**instance_info,**{"Tag": config.tag}})
            
            _instance_info={}
            for i in range(len(default_fileds)):
                _instance_info[default_fileds[i]] = instance_info[fileds[i]]
            
            _instance_info["DbType"] = db_type
            _instance_info["Tag"]    = config.tag
            try:
                db[full_table_name].insert_one({"InstanceId":_instance_info["InstanceId"], "Tag": config.tag})
                #需要创建索引 createIndexes([{"InstanceId":1}],{"unique":true})
                #InstanceId为唯一值
                #不存在则插入
                db[table_name].update_one({"InstanceId":_instance_info["InstanceId"]},{"$set":_instance_info},True)
            except:
                logger_err.error(format_exc())
                break

