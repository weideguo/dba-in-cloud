# -*- coding: utf-8 -*-
import time
import sys
from threading import Thread,Lock
from multiprocessing import Queue,Process
from traceback import format_exc

from .wrapper import logger,logger_err

share_queue = Queue()        #如果使用类初始化，外部设置则不共享？
retry_queue = Queue()
all_thread_queue = Queue()   #用于线程计数
          
      
class ProcessControlThread(object):
    """
    多进程控制的线程并发，用于充分利用CPU
    """
    def __init__(self, target, process_num=1, thread_num=1, arg_type="single", queue_end_flag="EOF", is_join=True, is_retry=True):
        """
        target          调用的函数
        arg_type        如何处理队列传数据到调用的函数 single list dict， single当成单个参数 list通过*list转换 dict通过**dict
        process_num     进程数 
        thread_num      单个进程的线程数
        queue_end_flag  队列的结束标识，用于结束进程
        """
        
        self.target = target
        
        self.process_num = process_num
        self.thread_num = thread_num
        
        self.queue_end_flag = queue_end_flag
        self.arg_type = arg_type
        
        if arg_type not in ["single","list","dict"]:
            raise Exception("arg_type must one of [ single list dict ]")
        
        self.process_list = []
        self.is_join = is_join
        self.is_retry = is_retry

         
    def tsize(self):
        """获取正在运行的线程数"""
        return all_thread_queue.qsize()
    
    
    def __single_thread_exe(self, thread_queue, thread_control_queue):
        """
        单个线程的操作
        """
        arg = thread_queue.get()
        try:
            all_thread_queue.put(1)
            logger.debug("begin function use [ %s ]" % arg)
            if self.arg_type == "list":
                self.target(*arg)
            elif self.arg_type == "dict":
                self.target(**arg)
            else:
                self.target(arg)
                
            logger.debug("end function use [ %s ]" % arg)
        except:
            logger_err.error(format_exc())
            logger_err.error("fail function use [ %s ]" % arg)
            # 如果执行失败，则重新放入队列中
            if self.is_retry:
                retry_queue.put(arg)
                logger.debug("retry [ %s ]" % arg)
        finally:
            try:
                all_thread_queue.get(block=False)
            except:
                pass
            try:
                thread_control_queue.get(block=False) 
            except:
                pass
        
    
    def __process_thread_generate(self):
        """
        每个进程的操作 调用多线程
        """
        try:
            from queue import Queue
        except:
            from Queue import Queue
        thread_queue = Queue(self.thread_num)              #传递信息给线程
        thread_control_queue = Queue(self.thread_num)      #线程并发控制
        while True:
            arg = share_queue.get()
            if arg == self.queue_end_flag:
                logger.debug("single processs exit, backgound thread contiune")
                break
            thread_control_queue.put(arg)             #超过会被阻塞
            thread_queue.put(arg)
            
            t = Thread(target = self.__single_thread_exe, args = (thread_queue, thread_control_queue))
            t.start()            
        
    
    def start(self):
        
        for i in range(self.process_num): 
            p = Process(target = self.__process_thread_generate, args = ())
            self.process_list.append(p)
        
        for p in self.process_list:
            p.start()
        
        if self.is_join:
            self.join()
       
    def join(self):   
        for p in self.process_list:
            p.join()
       
       
from queue import Queue
class FrequencyQueue(Queue):
    """
    带频率控制的队列 线程安全
    """
    
    def __init__(self, maxsize, count, time_space=1, error_logger=print):
        """
        maxsize      队列最大长度
        count        次数，即在指定时间间隔内可以put的次数
        time_space   时间间隔
        """
        super(FrequencyQueue, self).__init__(maxsize)
        self.maxsize    = maxsize 
        self.count      = count 
        self.time_space = time_space
        self.put_list   =[]
        self.error_logger = error_logger
        self.lock = Lock()
        
    def put(self, *args, **kwargs):
        with self.lock:
            if len(self.put_list) >= self.count:
                gap = self.time_space - (time.time() - self.put_list[0])
                if gap>0:
                    if self.error_logger:
                        self.error_logger("frequency limit, will wait %s seconds" % gap)
                    time.sleep(gap)
                
                self.put_list = self.put_list[1:]
                
            self.put_list.append(time.time())
            
            super(FrequencyQueue, self).put(*args,**kwargs)  

        