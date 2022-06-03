# -*- coding: utf-8 -*-
#一些函数的封装，以及为了单例的初始化
import sys
from .logger import simple_logger

log_level=10                        #参考logging模块的值 logging.DEBUG=10 logging.INFO=20
logger=simple_logger("standard",sys.stdout,log_level)
logger_err=simple_logger("error",sys.stderr,log_level)

