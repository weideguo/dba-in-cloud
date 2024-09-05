#!/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from libs import utils

# 请从腾讯云平台获取一个下载的url
url='http://xxxxx.myqcloud.com/xxxxx'
filename="xxxx.tar"
path="/data/backup/pg/full"


utils.download_file_ex(url, path, filename=filename)
