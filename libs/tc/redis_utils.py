# -*- coding: utf-8 -*-
#
# weideguo@dba 20210818
#
import json

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException 
from tencentcloud.redis.v20180412 import redis_client, models 

from libs.utils import date_add

class RedisOpt(object):
    """
    redis操作类
    """
    def __init__(self, id, key, endpoint="redis.tencentcloudapi.com"):
        
        self.id = id
        self.key = key
        self.endpoint = endpoint
        
        #尽可能列出所涉及的可用区
        self.region_list = [
        "ap-bangkok"
        ,"ap-beijing"
        ,"ap-chengdu"
        ,"ap-chongqing"
        ,"ap-guangzhou"
        ,"ap-hongkong"
        ,"ap-jakarta"
        ,"ap-mumbai"
        ,"ap-nanjing"
        ,"ap-seoul"
        ,"ap-shanghai"
        ,"ap-shanghai-fsi"
        ,"ap-shenzhen-fsi"
        ,"ap-singapore"
        ,"ap-tokyo"
        ,"eu-frankfurt"
        ,"eu-moscow"
        ,"na-ashburn"
        ,"na-siliconvalley"
        ,"na-toronto"
        ]
        
    
    def init_client(self, region):
        cred = credential.Credential(self.id, self.key) 
        httpProfile = HttpProfile()
        httpProfile.endpoint = self.endpoint
        
        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        return redis_client.RedisClient(cred, region, clientProfile) 

    
    def get_all_instances_info(self, offset=10, subnet_ids=[], *args, **kwargs):
        """
        获取所有区域的redis实例信息
        """
        all_instances=[]
        for region in self.region_list:
            #print(region)
            client = self.init_client(region)
            req = models.DescribeInstancesRequest()     
            
            i=0
            region_instance_list=[]
            while i>=0:
                req.from_json_string('''{"Region":"%s", "Offset":%s, "Limit": %s, "SubnetIds": %s}''' % (region, i*offset, offset, json.dumps(subnet_ids)))
                # subnet_ids 为数字，但要写成字符串格式（接口如此要求），可以先通过该接口获取所有数据库信息从而获取对应的子网
                resp = client.DescribeInstances(req)  
                #print(resp)
                #break
            
                region_instance_list += resp.InstanceSet
                if (i+1)*offset < resp.TotalCount:
                    i += 1
                else:
                    i = -1
            
            if len(region_instance_list):
                all_instances.append((region,region_instance_list))
                
        return all_instances

    
    def describe_backups(self, region, instance_id, backup_date, offset=10, max_try=5, *args, **kwargs):
        """查询备份ID"""
        client = self.init_client(region)
        req = models.DescribeInstanceBackupsRequest() 
        
        i = 0
        while i>=0 and i<max_try:
            req.from_json_string('''{"Offset":%s, "Limit":%s, "Region":"%s", "InstanceId":"%s"}''' % (i*offset,offset,region,instance_id))        
            resp = client.DescribeInstanceBackups(req)  

            backup_list = resp.BackupSet
            
            if backup_date == "lastest":
                backup_info = backup_list[0]
                i = -1
            else:
                for _backup_info in backup_list:
                    _backup_date = _backup_info.StartTime.split(" ")[0]
                    if _backup_date == backup_date:
                        backup_info = _backup_info
                        i = -1 
                        break
                    elif _backup_date < backup_date:
                        i = -1
                        break
            
            i = i+1 if i>=0 else -1
        
        return  backup_info
 
    
    def describe_backup_url(self, region, instance_id, backup_id):
        """查询获取下载的url"""
        client = self.init_client(region)
        req = models.DescribeBackupUrlRequest() 
        req.InstanceId = instance_id
        req.BackupId = backup_id
        
        resp = client.DescribeBackupUrl(req)  
        
        download_url = None
        try:
            download_url = resp.InnerDownloadUrl[0]
        except:
            print(resp)
            pass
        
        return download_url    
