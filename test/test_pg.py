#!/bin/env python3
# -*- coding: utf-8 -*-
#
import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config
from libs.tc import PgOpt

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
    
    po = PgOpt(id,key)
    po.region_list=region_list
    
    def test1():
        all_instances = po.get_all_instances_info()
        
        for region,region_instance_list in all_instances:
            print("region %s:"%region)
            for l in region_instance_list:
                #print(l)
                print("%s,%s,%s,%s,%s" % (l.DBInstanceName, l.Zone, l.DBInstanceType, str(l.DBInstanceCpu)+"C"+str(l.DBInstanceMemory)+"G"+str(l.DBInstanceStorage)+"G", l.TagList))
        #    print("-------------------")
    
    def test11():
        all_instances = po.get_all_instances_info()
        match_field   = ["DBInstanceId","DBInstanceName"]
        instance = "postgres-xxx2ixrf"
        # instance = ""
        _instance_info = po.get_instance_info(instance, all_instances, match_field, match_len=1)
        #print(all_instances)
        print(_instance_info)

    
    def test2():
        region = "ap-guangzhou"
        instance_id = "postgres-xxx2ixrf"
        binlog_time_after = "2022-03-07 10:00:00"
        binlog_time_max = "2022-03-07 10:02:00" 
        r = po.get_binlogs_url_for_instance(region, instance_id, binlog_name="", binlog_name_after="", binlog_time_after=binlog_time_after, binlog_time_max=binlog_time_max, max_try=100)
        print(r)
        
        """
        _name = re.search("(?<=%2F).+?tar.gz",url).group().split("%2F")[-1]
        # 因为文件是以tar格式存储的，并没有压缩
        name = _name[:-3] if _name[-3:]==".gz" else _name
        """

    def test21():
        region = "ap-guangzhou"
        instance_id = "postgres-xxx2ixrf"
        binlog_name = "000000010000006500000047.tar"
        r = po.get_binlogs_url_for_instance(region, instance_id, binlog_name=binlog_name,  max_try=100)
        print(r)


    def test3():
        region = "ap-guangzhou"
        instance_id = "postgres-xxx2ixrf"
        filter_date = "lastest"
        #filter_date = "2022-01-25"
        r = po.get_backup_url_for_instance(region, instance_id, filter_date)
        print(r)
    
    
    
    #test1()
    test2()
    #test3()
    #test11()
    #test21()
    
    
    
    
    
    
    
    
    
