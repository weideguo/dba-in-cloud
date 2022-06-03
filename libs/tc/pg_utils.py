# -*- coding: utf-8 -*-
#
# weideguo@dba 20220125
#
import re
import json
import time
import pytz
from datetime import datetime

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException 
from tencentcloud.postgres.v20170312 import postgres_client, models 

from libs.utils import date_add


def format_filename(url):
    """
    从url中获取文件，因为文件是以tar格式存储的，并没有压缩，url给的文件名有
    url="http://postgres-backup-gz-1300951326.cos.ap-guangzhou.myqcloud.com/pgsql%2F1012494%2Fxlog%2F2022-03-02%2F20220302120911_20220302120911-000000020000004D000000B7_000000020000004D000000B7.tar.gz?q-sign-algorithm=sha1&q-ak=AKIDa9jU0IrGva0B8zk90gAQRZVDk283nTCh&q-sign-time=1646216155%3B1646219755&q-key-time=1646216155%3B1646219755&q-header-list=host&q-url-param-list=&q-signature=577c507ed60591402b0b09b22ee40a4781bcdf3b"
    """
    # _name = re.search("(?<=%2F).+?tar.gz",url).group().split("%2F")[-1]
    # _name = re.search("(?<=%2F).+?tar.gz",url).group().split("%2F")[-1].split("_")[-1]
    # return _name[:-3] if _name[-3:]==".gz" else _name
    # url = "http://postgres-backup-gz-1300951326.cos.ap-guangzhou.myqcloud.com/pgsql%2F1012497%2Fxlog%2F2022-03-03%2F20220303144120_20220303144125-000000010000006400000096_000000010000006400000097.tar.gz?q-sign-algorithm=sha1&q-ak=AKIDa9jU0IrGva0B8zk90gAQRZVDk283nTCh&q-sign-time=1646357723%3B1646361323&q-key-time=1646357723%3B1646361323&q-header-list=host&q-url-param-list=&q-signature=17551b21b1e3be1ad833e771a1afdfdba9863150"
    # 存在这种情况
    _name = re.search("(?<=%2F).+?tar.gz",url).group().split("%2F")[-1].split("-")[-1].split("_")[0]
    return _name+".tar"
    

class PgOpt(object):
    """
    pg操作类
    """
    def __init__(self, id, key, endpoint="postgres.tencentcloudapi.com"):
        
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
        self.tz = pytz.timezone("Asia/Shanghai")
    
    
    def init_client(self, region):
        cred = credential.Credential(self.id, self.key) 
        httpProfile = HttpProfile()
        httpProfile.endpoint = self.endpoint
        
        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        return postgres_client.PostgresClient(cred, region, clientProfile) 

    
    def get_all_instances_info(self, offset=100, *args, **kwargs):
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
                req.from_json_string('''{"Offset":%s,  "Limit": %s}''' % (i*offset,offset))
                resp = client.DescribeDBInstances(req)        
                region_instance_list += resp.DBInstanceSet
                if (i+1)*offset < resp.TotalCount:
                    i += 1
                else:
                    i = -1
            
            if len(region_instance_list):
                all_instances.append((region,region_instance_list))
                
        return all_instances

    
    def get_binlogs_url_for_instance(self,*args, **kwargs):
        return self.get_xlogs_url_for_instance(*args, **kwargs)
    
    
    def get_xlogs_url_for_instance(self, region, instance_id, binlog_name="", binlog_name_after="", binlog_time_after="", binlog_time_max="", offset=10, max_try=5, url=None, *args, **kwargs):
        """
        获取指定实例的xlog下载列表 
        获取的队列应该按照 StartTime 逆序排序
        region, instance_id,  offset, max_try, binlog_time_after, binlog_time_max 只有这几个参数有效，其他参数只是为了兼容
        url 如果传入，重新获取该url（因为url会有过期的情况）
        """
        client = self.init_client(region)
        req = models.DescribeDBXlogsRequest()    
        
        # 没有则取两天前
        start_time = binlog_time_after or datetime.fromtimestamp(time.time()-86400*7,self.tz).strftime("%Y-%m-%d %H:%M:%S")  
        
        # 没有则为当前时间+1小时
        end_time = binlog_time_max or datetime.fromtimestamp(time.time()+3600,self.tz).strftime("%Y-%m-%d %H:%M:%S") 
        
        i=0
        url_list=[]
        while i>=0 and i<max_try:
            # 这里的Offset为页，不是偏移量
            request_str = '''{"DBInstanceId":"%s","StartTime":"%s","EndTime":"%s","Offset":%s,"Limit":%s}''' % (instance_id, start_time, end_time, i, offset)
            #print(request_str)
            req.from_json_string(request_str)
            
            resp = client.DescribeDBXlogs(req)        
            
            #小于全部长度
            if (i+1)*offset < resp.TotalCount:
                i += 1 
            else:
                i = -1   
            
            if url:
                for u in resp.XlogList:
                    if format_filename(u.InternalAddr) == format_filename(url):
                        return [u]

            elif binlog_name:
                for u in resp.XlogList:
                    if format_filename(u.InternalAddr) == binlog_name:
                        return [u]
                    elif format_filename(u.InternalAddr) < binlog_name:
                        print(request_str)
                        return []

            else:
                url_list += resp.XlogList
            
        return url_list

    
    def get_backup_url_for_instance(self, region, instance_id, filter_date, offset=10, max_try=5, *args, **kwargs): 
        """
        获取指定实例指定日期的全备下载url
        """
        client=self.init_client(region)
        req = models.DescribeDBBackupsRequest()
                
        if filter_date=="lastest":
            start_time = datetime.fromtimestamp(time.time()-86400*1,self.tz).strftime('%Y-%m-%d %H:%M:%S')  
            end_time = datetime.fromtimestamp(time.time()+3600,self.tz).strftime('%Y-%m-%d %H:%M:%S') 
        elif re.match("\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])", filter_date):
            start_time = "%s 00:00:00" % filter_date
            end_time = "%s 23:59:59" % filter_date
        else:
            raise Exception("variable filter_date must be a date type, for example: 2022-01-25 ")
        

    
        req.from_json_string('''{"DBInstanceId":"%s","Type":1, "StartTime":"%s", "EndTime":"%s","Offset":%s,"Limit":%s}''' % (instance_id, start_time, end_time, 0, offset))
       
        resp = client.DescribeDBBackups(req) 
        return resp.BackupList[0] if resp.BackupList else None

    
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
