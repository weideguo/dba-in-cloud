#!/bin/env python3
# -*- coding: utf-8 -*-
#
import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config
from libs.tc import CdbOpt

if __name__ == "__main__":
    
    id  = config.id
    key = config.key
    
    region_list = [
        #"ap-bangkok"
        "ap-beijing"
        ,"ap-chengdu"
        #,"ap-chongqing"
        ,"ap-guangzhou"
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
    
    co = CdbOpt(id,key)
    #print(co.region_list)
    co.region_list=region_list
    #print(co.region_list)
    
    def test1():
        #all_instances = co.get_all_instances_info(subnet_ids=[2618675,1994064,1115683])
        all_instances = co.get_all_instances_info(subnet_ids=[])
        #print(all_instances)
        
        for region,region_instance_list in all_instances:
            #print("region %s:"%region)
            for l in region_instance_list:
                print(l.InstanceName, l.InstanceId, l.InstanceType)
                #x
                #print(l.InstanceName,l.InstanceId, l.Vip, l.Vport)
                #print(l.InstanceName, l.InstanceId,l.Volume)
                #print(l.InstanceName, l.InstanceId,l.SubnetId)
        #    print("-------------------")
    

    def test2():
        region = "ap-shanghai"
        instance_id = "cdb-n3sfvxxx"
        
        #url_list = co.get_binlogs_url_for_instance(region, instance_id, binlog_time_after="2021-07-12 00:02:03")
        #url_list = co.get_binlogs_url_for_instance(region, instance_id, binlog_name_after=u"vag-kbb-mysql-prod_binlog_mysqlbin.025907 ")
        #url_list = co.get_binlogs_url_for_instance(region, instance_id, binlog_time_after="2021-12-01 18:01:10",binlog_time_max="2021-12-01 18:05:17",max_try=100)
        url_list = co.get_binlogs_url_for_instance(region, instance_id, binlog_name="vag-kbb-mysql-prod_binlog_mysqlbin.035076")
        print(url_list)
        #for u in url_list:
        #    print(u.BinlogStartTime,u.Name) 
    
    def test3():
        region = "ap-guangzhou"
        instance_id = "cdb-3dzk7xxx"
        filter_date = "2022-02-28"
        url_info = co.get_backup_url_for_instance(region, instance_id, filter_date)
        print(url_info)
        # region': 'ap-guangzhou', 'instance_id': 'cdb-3dzk7xxx', 'filter_date': '2022-02-24'
        
    def test4():
        all_instances = co.get_all_instances_info(subnet_ids=[])
        
        instance="cdb-n3sfvxxx"
        match_field=["InstanceId", "InstanceName"]
        r=co.get_instance_info(instance, all_instances, match_field, match_len=1)
        print(r)

    #test1()
    #test2()
    test3()
    #test4()
