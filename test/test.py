import json
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.mongodb.v20190725 import mongodb_client, models
try:
    id  = "aaaaaaaaaaaaa"
    key = "bbbbbbbbbbbbbb"
    cred = credential.Credential(id, key)
    httpProfile = HttpProfile()
    httpProfile.endpoint = "mongodb.tencentcloudapi.com"

    clientProfile = ClientProfile()
    clientProfile.httpProfile = httpProfile
    client = mongodb_client.MongodbClient(cred, "ap-guangzhou", clientProfile)

    req = models.CreateBackupDownloadTaskRequest()
    params = {
        "InstanceId": "cmgo-xxxxxxxxxx",
        "BackupName": "cmgo-xxxxxxxxxx_2022-02-10 01:20",
        "BackupSets": [
            {
                "ReplicaSetId": "cmgo-xxxxxx_0"
            }
        ]
    }
    req.from_json_string(json.dumps(params))

    resp = client.CreateBackupDownloadTask(req)
    print(resp.to_json_string())

except TencentCloudSDKException as err:
    print(err)