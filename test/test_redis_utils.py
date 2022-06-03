#!/bin/env python3
# -*- coding: utf-8 -*-
#
import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config
from libs.tc import RedisOpt

if __name__ == "__main__":
    
    id  = config.id
    key = config.key
    
    region_list = [
        #"ap-bangkok"
        #"ap-beijing"
        #,"ap-chengdu"
        #,"ap-chongqing"
        #"ap-guangzhou"
        #,"ap-hongkong"
        #,"ap-jakarta"
        #,"ap-mumbai"
        #,"ap-nanjing"
        #,"ap-seoul"
        "ap-shanghai"
        #,"ap-shanghai-fsi"
        #,"ap-shenzhen-fsi"
        #,"ap-singapore"
        #,"ap-tokyo"
        #,"eu-frankfurt"
        #,"eu-moscow"
        #,"na-ashburn"
        #,"na-siliconvalley"
        #,"na-toronto"
        ]
    
    endpoint="redis.tencentcloudapi.com"  

    ro = RedisOpt(id,key,endpoint)
    #print(ro.region_list)
    ro.region_list=region_list
    #print(ro.region_list)
    
    def test1():
        #all_instances = ro.get_all_instances_info(subnet_ids=["3219169","1115683"])
        all_instances = ro.get_all_instances_info(subnet_ids=[])
        #print(all_instances)
        for region,region_instance_list in all_instances:
            print("region %s:"%region)
            for l in region_instance_list:
                #print(l.InstanceName,l.InstanceId, l.WanIp, l.Port)
                #print(l.InstanceName,l.SubnetId)
                #print(l.InstanceName,l.SlaveReadWeight)
                print(l)
            print("-------------------")

    def test2():
        region = "ap-shanghai"
        instance_id ="crs-xxx9dzh5"
        #backup_date ="2022-03-13"
        backup_date ="lastest"
        print(ro.describe_backups(region, instance_id, backup_date))
    
    def test3():
        region = "ap-shanghai"
        instance_id ="crs-xxx9dzh5"
        backup_id =  "254111520-5506313-1155158350"
        print(ro.describe_backup_url(region, instance_id, backup_id))
            
    #test1()
    #test2()
    test3()
