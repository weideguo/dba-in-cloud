#!/bin/env python
# -*- coding: utf-8 -*-
# 
# 检查pg xlog、mysql binlog文件的连续性
# weideguo@dba 20220303
# 
import os
import sys
from traceback import format_exc

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from config import config


def get_pg_xlog_separate_files(check_dir):
    """
    获取pg xlog不连续的文件
    """
    files=os.listdir(check_dir)
    files.sort()
    
    not_match_list = []
    
    _f=""
    for f in files:
        if not os.path.isfile(os.path.join(check_dir,f)):
            raise Exception("%s is not a file" % os.path.join(check_dir,f))
            
        if _f:
            # 0000000200000050000000FF.tar.gz
            # 000000020000005100000000.tar.gz
            # 后8位 前16位分开进位
            f1  = _f.split(".")[0][-8:] 
            f2  =  f.split(".")[0][-8:] 
            _f1 = _f.split(".")[0][:-8] 
            _f2 =  f.split(".")[0][:-8] 
            if (f1=="000000FF" and f2=="00000000" and int(_f2,16)-int(_f1,16) == 1) or ( _f2 == _f1 and int(f2,16)-int(f1,16) == 1):
                pass
            else:
                not_match_list.append((_f,f))
        
        _f = f
    
    return not_match_list

    
def get_mysql_binlog_separate_files(check_dir):
    """
    获取mysql binlog不连续的文件
    """
    files=os.listdir(check_dir)
    files.sort()
    
    not_match_list = []
    
    _f=""
    for f in files:
        if not os.path.isfile(os.path.join(check_dir,f)):
            raise Exception("%s is not a file" % os.path.join(check_dir,f))
        
        if _f:
            f1 = _f.split(".")[1] 
            f2 =  f.split(".")[1] 
            if int(f2,10)-int(f1,10) == 1:
                pass
            else:
                not_match_list.append((_f,f))
        
        _f = f
    
    return not_match_list
    
   
def check_pg_xlog():
    pg_xlog_base_dir = config.pg_binlog_base_dir
    #pg_xlog_base_dir = "/data/backup/pg/xlog"
    pg_not_matchs = []
    for d in os.listdir(pg_xlog_base_dir):
        _d = os.path.join(pg_xlog_base_dir,d)
        if os.path.isdir(_d):
            pg_not_match = get_pg_xlog_separate_files(_d)
            if pg_not_match:
                pg_not_matchs.append((d, pg_not_match))
                
        else:
            raise Exception("%s is not a dir" % _d)
    
    return pg_not_matchs
    
    
def check_mysql_binlog():
    binlog_base_dir = config.binlog_base_dir
    #binlog_base_dir = "/data/backup/mysql/binlog"
    
    mysql_not_matchs = []
    for d in os.listdir(binlog_base_dir):
        _d = os.path.join(binlog_base_dir,d)
        if os.path.isdir(_d):
            mysql_not_match = get_mysql_binlog_separate_files(_d)
            if mysql_not_match:
                mysql_not_matchs.append((d, mysql_not_match))
                
        else:
            raise Exception("%s is not a dir" % _d)
    
    return mysql_not_matchs


if __name__ == "__main__":
    pg_not_matchs = check_pg_xlog()
    mysql_not_matchs = check_mysql_binlog()
    #print(pg_not_matchs)
    for instance_name, pg_not_match in pg_not_matchs:
       print("-------------------%s------------------------------" % instance_name)
       for f_from,f_to in pg_not_match:
            #print(f_from,f_to)
            _f_from = f_from.split(".")
            _f_to   = f_to.split(".")
            
            f_from_post = _f_from[0][-2:] 
            f_to_post   =   _f_to[0][-2:] 
            
            f_from_pre_l = _f_from[0][:-2] 
            f_to_pre_l   =   _f_to[0][:-2] 
            
            f_from_pre = _f_from[0][16]
            f_to_pre   =   _f_to[0][16]
            
            if int(f_to_pre,16) - int(f_from_pre,16) > 1:
                # 间隔超过两个FF 则先不处理
                print(f_from,f_to)
            else:
                # f_to_post  大于 f_from_post 的情况
                if f_to_post > f_from_post:
                    gap = int(f_to_post,16) - int(f_from_post,16)                    
                else:
                    # 需要处理进位
                    gap = int("1"+f_to_post,16) - int(f_from_post,16)
                    
                    
                for i in range(1,gap):
                    if int(f_from_post,16)+i >= 256:
                        #需要进位
                        _f_0_sub = hex(int(f_from_post,16)+i-256).upper()[2:]
                        _f_0 = f_to_pre_l+ ("0"+_f_0_sub if len(_f_0_sub) == 1 else _f_0_sub)
                        print( ".".join([_f_0] + _f_to[1:]) )
                    else:
                        #不需进位
                        _f_0_sub = hex(int(f_from_post,16)+i).upper()[2:]
                        _f_0 = f_from_pre_l+ ("0"+_f_0_sub if len(_f_0_sub) == 1 else _f_0_sub)
                        print( ".".join([_f_0] + _f_from[1:]) )


    for instance_name, mysql_not_match in mysql_not_matchs:
       print("-------------------%s------------------------------" % instance_name)
       for f_from,f_to in mysql_not_match:
            print(f_from,f_to)

