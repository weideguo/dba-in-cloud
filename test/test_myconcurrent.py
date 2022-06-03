#!/bin/env python3
# -*- coding: utf-8 -*-
#
import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from libs.myconcurrent import ProcessControlThread,share_queue


if __name__ == "__main__":
    import time    
    import random
    def download_file(url,b=0):
        print("---------%s %s -----------" % (url,b))
        time.sleep(random.random()*10)     
        
    
    process_num = 2
    thread_num  = 3
    
    def test1():
        for i in range(20):
            share_queue.put(i)
        
        for i in range(process_num): 
            share_queue.put("EOF")                           #队列尾部设置结束标识符    
        
        pct=ProcessControlThread(download_file, process_num, thread_num, arg_type="single")
        pct.start()
        
        print("----------------------------------------------------")
    
    def test2():
        for i in range(20):
            share_queue.put([i,2])
            
        for i in range(process_num): 
            share_queue.put("EOF")                    
            
        pct=ProcessControlThread(download_file, process_num, thread_num, arg_type="list")
        pct.start()
        
        print("----------------------------------------------------")
    
    def test3():
        for i in range(20):
            share_queue.put({"url":i,"b":"bbb"})
        
        for i in range(process_num): 
            share_queue.put("EOF")
        
        pct=ProcessControlThread(download_file, process_num, thread_num, arg_type="dict")
        pct.start()
        
        print("----------------------------------------------------")
    

    #test1()
    #test2()
    test3()
