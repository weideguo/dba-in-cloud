# -*- coding: utf-8 -*-
#
# 存放配置信息，有些模块会热加载
# weideguo@dba 20210713

#云平台的id key
id  = "aaaaaaaaaaaaaaa"
key = "bbbbbbbbbbbbbbb"

#binlog备份存放的路径
binlog_base_dir    = "/data/backup/mysql/binlog"
pg_binlog_base_dir = "/data/backup/pg/xlog"

#全备的存放路径
backup_dir         = "/data/backup/mysql/full"  
pg_backup_dir      = "/data/backup/pg/full"  
mongodb_backup_dir = "/data/backup/mongodb/full"  
redis_backup_dir   = "/data/backup/redis/full"

#有些实列可以被排除 如异地灾灾备 测试使用 只填主库
#binlog排除
binlog_exclude_instance=[
"cdb-xxxxxxxxxx",    #            



]                     

#全备排除
backup_exclude_instance=[
"cdb-xxxxxxxxxx",    #           

]          

# 只获取指定子网的数据库信息
# 为空则获取所有的
# 通过接口 DescribeDBInstances 获取对应的子网id
mysql_subnetid = []
all_subnetid = []
     
#所涉及的可用区，如果没有使用，则注释，多余的配置会导致获取实例信息变慢      
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

#下载的并发控制 mysql
binlog_process_num = 4
binlog_thread_num = 4

# mysql pg mongodb
backup_process_num = 2
backup_thread_num = 2

# pg_xlog
xlog_process_num = 2
xlog_thread_num = 2

#用于存储信息的mongodb配置
mongodb_config = {
"uri": "mongodb://127.0.0.1:27017/dba_opt",
"db" : "dba_opt"
}

tag = "shanghai"
