#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 25 10:04:31 2021

@author: maahutch
"""

import subprocess


#files = os.system("hdfs dfs -find /WoSraw_2020_all/parquet")

files = subprocess.Popen(["hdfs", "dfs", "-find", "/WoSraw_2020_all/parquet"])

files2 = files.decode('utf-8')

files3 = files2.split('/n')

files4 = files3[2:]
