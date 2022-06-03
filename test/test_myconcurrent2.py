#!/bin/env python3
# -*- coding: utf-8 -*-
#
import os
import sys

base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

from libs.myconcurrent import FrequencyQueue


if __name__ == "__main__":

    def test1():
        fq = FrequencyQueue(300,5)
        
        for i in range(30):
            fq.put(1)

    def test2():
        fq = FrequencyQueue(300,5,2)
        
        for i in range(30):
            fq.put(1)
            
    #test1()
    test2()