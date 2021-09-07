#!/bin/bash

pyspark --driver-memory 26G --executor-memory 26G --executor-cores 8 < wosQueryforTavernier.py
