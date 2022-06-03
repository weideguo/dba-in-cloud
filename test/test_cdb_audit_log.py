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
        #"ap-beijing"
        #,"ap-chengdu"
        #,"ap-chongqing"
        "ap-guangzhou"
        #,"ap-hongkong"
        #,"ap-jakarta"
        #,"ap-mumbai"
        #,"ap-nanjing"
        #,"ap-seoul"
        #,"ap-shanghai"
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
        region       = "ap-guangzhou"
        instance_id  = "cdb-2e2lvxxx"
        #start_time   = "2021-11-30 12:00:00"
        #end_time     = "2021-12-30 12:00:00"
        start_time   = "2021-12-01 12:00:00"
        end_time     = "2021-12-01 12:01:00"
        Filter       = {"Host": [ "10.21.0.42", "10.21.0.85" ],"SqlTypes": [ "SELECT" ]}
    
        r=co.create_audit_log(region, instance_id, start_time, end_time, Filter)
        print(r)
    
    def test2():
        region       = "ap-guangzhou"
        instance_id  = "cdb-2e2lvxxx"
        file_name    = "100022602329_cdb-2e2lvxxx_1638438694_7.csv"
        r=co.get_audit_log_url(region, instance_id, file_name)
        print(r)
    
    
    #test1()
    test2()