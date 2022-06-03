#coding:utf8
# -*- coding: utf-8 -*-
#!/bin/env python3
#
# 用于从腾讯云下载binlog备份，单个文件下载，用于手动补充失败的文件
# weideguo@dba 20211201
#
import os
import sys
import optparse

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from libs.tc import CdbOpt
from libs.utils import download_file_ex
from config import config



def main(instance, binlog, filename):
    co = CdbOpt(config.id, config.key)
    co.region_list = config.region_list
    all_instances = co.get_all_instances_info(subnet_ids=config.mysql_subnetid)
    match_field   = ["InstanceId","InstanceName"]
    _instance_info = co.get_instance_info(instance, all_instances, match_field, match_len=1)
    if len(_instance_info) > 1:
        raise Exception("filter instance_info too long %s" % _instance_info)
    
    instance_info=_instance_info[0]
    
    _region     = instance_info.Region   
    instance_id = instance_info.InstanceId
    instance_name = instance_info.InstanceName
    
    url_list = co.get_binlogs_url_for_instance(_region, instance_id, binlog_name=binlog, max_try=1000)
    
    if len(url_list)!=1 or not url_list[0].IntranetUrl:
        raise Exception("donwload info error %s" % url_list)
    
    download_url = url_list[0].IntranetUrl
    path = os.path.join(config.binlog_base_dir, instance_name)
    if not filename:
        filename = "%s_%s_%s" % (url_list[0].BinlogStartTime.replace(" ","").replace(":","").replace("-",""), url_list[0].BinlogFinishTime.replace(" ","").replace(":","").replace("-",""), url_list[0].Name)
    print("begin download %s" %  filename)
    download_file_ex(url=download_url, path=path, filename=filename)


def arg_parse():
    """
    命令行参数解析
    """
    usage = "Usage: %prog [options]"
    usage += "\n\nstart with following options:"
    
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-i", "--instance", help=u"只获取该实例的备份，可为实例id或者实例名（必须）")
    parser.add_option("-b", "--binlog",   help=u"要下载的binlog文件名（必须）")
    parser.add_option("-f", "--filename", default="", help=u"保存的文件名，为空则使用下载时传过来的文件名")
    
    return parser.parse_args()    


if __name__ == "__main__":
    options, args = arg_parse()
    instance      = options.instance 
    binlog        = options.binlog
    filename      = options.filename
    
    if not instance or not  binlog:
        raise Exception("must input enought vars")

    main(instance, binlog, filename=filename)
