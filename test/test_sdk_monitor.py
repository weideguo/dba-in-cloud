#!/bin/env python3
# -*- coding: utf-8 -*-

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException 
from tencentcloud.monitor.v20180724 import monitor_client, models 

import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config

id  = config.id
key = config.key


region = "ap-shanghai"


cred = credential.Credential(id, key) 
httpProfile = HttpProfile()
httpProfile.endpoint = "monitor.tencentcloudapi.com"

clientProfile = ClientProfile()
clientProfile.httpProfile = httpProfile
client = monitor_client.MonitorClient(cred, region, clientProfile) 

req = models.GetMonitorDataRequest()   
"""
参考
https://cloud.tencent.com/document/product/248/6843
QCE/REDIS
QCE/REDIS_MEM

QCE/CMONGO
"""
req.Namespace = "QCE/CDB"
###req.MetricName = "RealCapacity"   #磁盘使用空间 不含 binlog undolog等日志的data目录
req.MetricName = "VolumeRate"        #空间利用率  磁盘使用空间/实例购买空间
#req.MetricName = "CpuUseRate"       #cpu使用率 %
#req.MetricName = "MemoryUseRate"    #qps
#req.MetricName = "Qps"
#req.MetricName = "Tps"

req.Period  = 300
req.StartTime = '2021-10-14T10:24:53+08:00' #"2021-06-13T00:00:00+08:00"
#req.EndTime =

req.Instances = [{"Dimensions": [{"Name":"InstanceId","Value":"cdbro-0qtjc1be"}]},
{"Dimensions": [{"Name":"InstanceId","Value":"cdb-h23s2utm"}]}
]


resp = client.GetMonitorData(req)        


print(resp.to_json_string()) 

