#!/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from libs import utils

# 请从腾讯云平台获取一个下载的url
url='http://postgres-backup-gz-1300000000.cos.ap-guangzhou.myqcloud.com/pgsql%2F1012492%2Fdata%2F2022-02-17%2Fautomatic-20220217020059.tar.gz?q-sign-algorithm=sha1&q-ak=AKIDa9jU0IrGva0B8zk90gAQRZVDk283nTCh&q-sign-time=1645170891%3B1645174491&q-key-time=1645170891%3B1645174491&q-header-list=host&q-url-param-list=&q-signature=61a7de17d017fb5f9bc9420222f5da944f43ae2d'
filename="20220217020059_ecoo-comm-server-comm.tar"
path="/data/backup/pg/full"


utils.download_file_ex(url, path, filename=filename)
