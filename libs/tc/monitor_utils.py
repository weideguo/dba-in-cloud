# -*- coding: utf-8 -*-
#
# weideguo@dba 20210714
#

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException 
from tencentcloud.monitor.v20180724 import monitor_client, models 


class MonitorOpt(object):
    """
    监控信息操作类
    """
    def __init__(self, id, key, region, endpoint="monitor.tencentcloudapi.com"):
        self.id = id
        self.key = key
        self.endpoint = endpoint
        self.region = region
        
    
    def init_client(self):
        cred = credential.Credential(self.id, self.key) 
        httpProfile = HttpProfile()
        httpProfile.endpoint = self.endpoint
        
        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        #这个对象不支持并发复用，必须每个并发创建一个
        return monitor_client.MonitorClient(cred, self.region, clientProfile) 
    
    
    def get_monitor_data(self, instances, metricname, starttime, endtime, period=300,namespace="QCE/CDB"):
        """
        instances:
        [{"Dimensions": [{"Name":"InstanceId","Value":"cdbro-0qtjc1be"}]},
        {"Dimensions": [{"Name":"InstanceId","Value":"cdb-h23s2utm"}]}
        ]
        
        metricname:
        ##"RealCapacity"     #磁盘使用空间 不含 binlog undolog等日志的data目录
        "VolumeRate"       #空间利用率  磁盘使用空间/实例购买空间
        "CpuUseRate"       #cpu使用率 %
        "MemoryUseRate"    #内存使用率
        "Qps"
        "Tps"
        
        starttime endtime:
        #"2021-06-13T00:00:00+08:00"
        
        period 指标统计间隔
        """
        
        req = models.GetMonitorDataRequest()   
        
        req.Namespace = namespace
        req.MetricName = metricname      
        req.Period  = period                     
        req.StartTime = starttime       
        req.EndTime = endtime
        
        req.Instances = instances
        
        return self.init_client().GetMonitorData(req)        
        