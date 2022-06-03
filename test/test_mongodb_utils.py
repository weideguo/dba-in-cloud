#!/bin/env python3
# -*- coding: utf-8 -*-
#
import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config
from libs.tc import MongodbOpt

if __name__ == "__main__":
    
    id  = config.id
    key = config.key
    
    region_list = [
        #"ap-bangkok"
        #"ap-beijing"
        #,"ap-chengdu"
        #,"ap-chongqing"
        "ap-guangzhou"
        #,"ap-hongkong"
        #,"ap-jakarta"
        #,"ap-mumbai"
        #,"ap-nanjing"
        #,"ap-seoul"
        ,"ap-shanghai"
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
    
    endpoint="mongodb.tencentcloudapi.com"  

    do = MongodbOpt(id,key,endpoint)
    #print(do.region_list)
    do.region_list=region_list
    print(do.region_list)
    
    def test1():
        all_instances = do.get_all_instances_info(project_ids=[0])
        #print(all_instances)
        for region,region_instance_list in all_instances:
            print("region %s:"%region)
            for l in region_instance_list:
                #print(l.InstanceName,l.InstanceId, l.Vip, l.Vport)
                #print(l.InstanceName,l.SubnetId,)
                #print(l.InstanceName,l.ProjectId)
                print(l)
                break1
            print("-------------------")


    def test2():
        
        region = "ap-guangzhou"
        instance_id = "cmgo-xxxf7xxx"
        backup_date = "lastest"
        backup_date = "2022-03-08"
        print(do.describe_backups(region, instance_id, backup_date))
        
    
    def test3():
        region = "ap-guangzhou"
        instance_id = "cmgo-xxxf7xxx"
        backup_name = "cmgo-xxxf7xxx_2022-02-11 01:18"
        print(do.create_backup_download_task(region, instance_id, backup_name))

    
    def test4():
        region = "ap-guangzhou"
        instance_id = "cmgo-xxxf7xxx"
        backup_name = "cmgo-xxxf7xxx_2022-02-11 01:18"
        print(do.describe_backup_download_task(region, instance_id, backup_name))
    
    
    #test1()
    test2()
    #test3()
    #test4()
    
"""

ap-guangzhou cmgo-xxxf7yh1 xxxx-base-bridge-prod 2022-02-11

ap-guangzhou cmgo-xxx141id xxxx-k8s-common-prod 2022-02-11

ap-guangzhou cmgo-xxx9kmbz xxxx-k8s-xxxx-prod 2022-02-11

ap-guangzhou cmgo-xxxzcaxv xxxx-k8s-qifu-prod 2022-02-11

ap-guangzhou cmgo-xxxw6hu1 xxxxprod-spiderkeeper 2022-02-11

ap-guangzhou cmgo-xxxour6z xxxx-k8s-xxxx-prod 2022-02-11

ap-shanghai cmgo-xxxbj9e3 cmgo-xxxbj9e3 2022-02-11
"""