# -*- coding: utf-8 -*-
#
# weideguo@dba 20210712
#
import json

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException 
from tencentcloud.cdb.v20170320 import cdb_client, models 

from libs.utils import date_add

class CdbOpt(object):
    """
    cdb操作类
    """
    def __init__(self, id, key, endpoint="cdb.tencentcloudapi.com"):
        
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
        return cdb_client.CdbClient(cred, region, clientProfile) 

    
    def get_all_instances_info(self, offset=2000, subnet_ids=[], *args, **kwargs):
        """
        获取所有区域的CDB实例信息
        """
        all_instances=[]
        for region in self.region_list:
            #print(region)
            client = self.init_client(region)
            req = models.DescribeDBInstancesRequest()     
            
            i=0
            region_instance_list=[]
            while i>=0:
                req.from_json_string('''{"Offset":%s,  "Limit": %s, "SubnetIds": %s }''' % (i*offset,offset,subnet_ids))
                # SubnetIds 为空则获取所有，否则只获取指定子网的信息，可以先通过该接口获取所有数据库信息从而获取对应的子网
                resp = client.DescribeDBInstances(req)        
            
                region_instance_list += resp.Items
                if (i+1)*offset < resp.TotalCount:
                    i += 1
                else:
                    i = -1
            
            if len(region_instance_list):
                all_instances.append((region,region_instance_list))
                
        return all_instances

    
    def get_binlogs_url_for_instance(self, region, instance_id, binlog_name="", binlog_name_after="", binlog_time_after="", binlog_time_max="", offset=10, max_try=5, *args, **kwargs):
        """
        获取指定实例的binlog下载列表
        获取的队列应该按照 Name 以及  "BinlogStartTime" 逆序排序
        设置一个迭代次数，要不然可能会一直翻到太久之前的
        binlog_name 优先于 binlog_name_after 优先于 binlog_time_after 
        
        binlog_time_max 下载的binlog时间最高包含
        """
        client = self.init_client(region)
        req = models.DescribeBinlogsRequest()     
        
        i=0
        url_list=[]
        while i>=0 and i<max_try:
            #print(i)
            req.from_json_string('''{"InstanceId":"%s","Offset":%s,"Limit":%s}''' % (instance_id, i*offset, offset))
        
            resp = client.DescribeBinlogs(req)        
               
            #小于全部长度
            if (i+1)*offset < resp.TotalCount:
                i += 1 
            else:
                i = -1   
               
            for ri in resp.Items:
                if binlog_time_max:
                    if ri.BinlogStartTime > binlog_time_max:
                        # 如果备份开始的时间大于最大需要的时间，则跳过判断下一个条目
                        continue
                
                if not binlog_name and not binlog_name_after and not binlog_time_after:
                    url_list.append(ri)
                elif binlog_name:
                    if ri.Name == binlog_name:
                        url_list.append(ri)
                        i = -1
                    elif ri.Name < binlog_name:
                        i = -1
                    
                elif binlog_name_after:
                    #文件名只要大于
                    if ri.Name > binlog_name_after :
                        url_list.append(ri)
                    else:
                        i = -1
                elif binlog_time_after:
                    # 只要备份的结束时间大于等于所需要的开始时间，即可任务该binlog是需要的
                    if ri.BinlogFinishTime >= binlog_time_after :
                        url_list.append(ri)
                    else:
                        i = -1
        
        return url_list

    
    def get_backup_url_for_instance(self, region, instance_id, filter_date, offset=10, max_try=5, *args, **kwargs): 
        """
        获取指定实例指定日期的全备下载url
        """
        client=self.init_client(region)
        req = models.DescribeBackupsRequest()
        
        i=0
        while i>=0 and i<max_try:
            req.from_json_string('''{"InstanceId":"%s","Offset":%s,"Limit":%s}''' % (instance_id, i*offset, offset))
            
            resp = client.DescribeBackups(req) 
            #print(resp)
            if (i+1)*offset < resp.TotalCount:
                i += 1 
            else:
                i = -1
                
            for download_info in resp.Items:
                if filter_date=="lastest":
                    #print("yes, i will return lastest backup url")
                    #返回第一个，即最新的备份
                    return download_info
            
                #print(download_info.Date, filter_date, date_add(filter_date,1))
                if download_info.Date > filter_date and download_info.Date < date_add(filter_date,1):
                    #要当天的最新备份
                    
                    return download_info
                    #download_url = download_info.IntranetUrl
                    
                if download_info.Date < filter_date:
                    #小于过滤时间的备份不需要再判断
                    i = -1
        
        return {}


    def get_instance_info(self, instance, all_instances, match_field, match_len=1, *args, **kwargs):
        """
        过滤获取指定实例的详细信息
        """
        instance_info = []
        for region,region_instance_list in all_instances:
            if match_len and len(instance_info)>=match_len:
                break
            for l in region_instance_list:
                if instance in [json.loads(l.to_json_string()).get(m,"") for m in match_field]:
                        instance_info.append(l)
                        _region = region
                        if match_len and len(instance_info)>=match_len:
                            break
                            
        return  instance_info


    def create_audit_log(self, region, instance_id, start_time, end_time, filter={}, *args, **kwargs):   
        """
        生成指定实例审计的审计日志，会立即返回，但下载地址可能还未生成
        参考 https://cloud.tencent.com/document/product/236/45454
        """
        client=self.init_client(region)
        req = models.CreateAuditLogFileRequest()
        
        params = {
            "InstanceId": instance_id,
            "StartTime": start_time,
            "EndTime": end_time,
            "Order": "DESC",
            "OrderBy": "timestamp",
            "Filter": filter,
            #"Filter": {
            #    "Host": [ "10.21.0.42"],
            #    "SqlTypes": [ "SELECT" ]
            #}
        }
        req.from_json_string(json.dumps(params))
        
        resp = client.CreateAuditLogFile(req)

        r = ""
        try:
            r = resp.FileName
        except: 
            raise Exception(resp.to_json_string())
        
        return r
        
        
    def get_audit_log_url(self, region, instance_id, file_name, *args, **kwargs):   
        """
        获取指定实例审计日志的下载地址
        """
        client=self.init_client(region)
        req = models.DescribeAuditLogFilesRequest()
        
        params = {
            "InstanceId": instance_id,
            "FileName": file_name
        }
        req.from_json_string(json.dumps(params))
        
        resp = client.DescribeAuditLogFiles(req)
        
        # 不应该存在获取的数量超过1
        if not resp.Items:
            r = []
        elif len(resp.Items)==1:
            r = resp.Items[0]
        else:
            raise Exception(resp.to_json_string())    
        
        return r
