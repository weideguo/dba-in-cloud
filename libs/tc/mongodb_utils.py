# -*- coding: utf-8 -*-
#
# weideguo@dba 20210818
#

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException 
from tencentcloud.mongodb.v20190725 import mongodb_client, models 

from libs.utils import date_add

class MongodbOpt(object):
    """
    mongodb操作类
    """
    def __init__(self, id, key, endpoint="mongodb.tencentcloudapi.com"):
        
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
        return mongodb_client.MongodbClient(cred, region, clientProfile) 

    
    def get_all_instances_info(self, offset=10, project_ids=[], *args, **kwargs):
        """
        获取所有区域的mongodb实例信息
        """
        all_instances=[]
        for region in self.region_list:
            #print(region)
            client = self.init_client(region)
            req = models.DescribeDBInstancesRequest()     
            
            i=0
            region_instance_list=[]
            while i>=0:
                req.from_json_string('''{"Offset":%s,  "Limit": %s, "ProjectIds": %s}''' % (i*offset,offset,project_ids))
                # 可以先通过该接口获取所有数据库信息从而获取对应的项目id
                resp = client.DescribeDBInstances(req)  
                #print(resp)
                #break
            
                region_instance_list += resp.InstanceDetails
                if (i+1)*offset < resp.TotalCount:
                    i += 1
                else:
                    i = -1
            
            if len(region_instance_list):
                all_instances.append((region,region_instance_list))
                
        return all_instances

    
    def describe_backups(self, region, instance_id, backup_date, offset=10, max_try=5, *args, **kwargs):
        """查询备份名"""
        client = self.init_client(region)
        req = models.DescribeDBBackupsRequest()   
        
        backup_method = 2    #物理备份？只有这个
        
        backup_name = None
        
        i = 0
        while i>=0 and i<max_try:
            req.from_json_string('''{"BackupMethod":%s, "Offset":%s, "Limit":%s, "Region":"%s", "InstanceId":"%s"}''' % (backup_method,i*offset,offset,region,instance_id))        
            resp = client.DescribeDBBackups(req)  
            
            # 因为获取到的结果集合是顺序的
            backup_list = resp.BackupList[::-1]
            
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
    
       
    def create_backup_download_task(self, region, instance_id, backup_name, *args, **kwargs):
        """由备份名创建下载任务"""
        client = self.init_client(region)
        req = models.CreateBackupDownloadTaskRequest() 
        
        #print(region, instance_id, backup_name)
        req.InstanceId = instance_id
        req.BackupName = backup_name
        req.BackupSets = [{
                             "ReplicaSetId": instance_id+"_0"
                          },
                         ]
        resp = client.CreateBackupDownloadTask(req)  
        
        return resp
    
    
    def describe_backup_download_task(self, region, instance_id, backup_name, *args, **kwargs):
        """查询获取下载的url"""
        client = self.init_client(region)
        req = models.DescribeBackupDownloadTaskRequest() 
        req.InstanceId = instance_id
        req.BackupName = backup_name
        
        resp = client.DescribeBackupDownloadTask(req)  
        download_url = None
        
        try:
            download_url = resp.Tasks[0].Url
        except:
            print(resp)
            pass
        
        return download_url

