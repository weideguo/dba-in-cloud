#!/bin/env python3
# -*- coding: utf-8 -*-

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException 
from tencentcloud.cdb.v20170320 import cdb_client, models 

import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config

id  = config.id
key = config.key


region = "ap-shanghai"

params = '''{"InstanceId":"cdb-xxxs2utm","Limit": 2}'''
#params = '''{"InstanceId":"cdb-xxxfvfs2","Limit": 2}'''

#DescribeBinlogs
#DescribeDBInstances

try: 
    cred = credential.Credential(id, key) 
    httpProfile = HttpProfile()
    #httpProfile.endpoint = "cdb.tencentcloudapi.com"

    clientProfile = ClientProfile()
    clientProfile.httpProfile = httpProfile
    client = cdb_client.CdbClient(cred, region, clientProfile) 

    req = models.DescribeDBInstancesRequest()     ##
    
    req.from_json_string(params)

    resp = client.DescribeDBInstances(req)        ##
    
    
    print(resp.to_json_string()) 

except TencentCloudSDKException as err: 
    print(err) 
