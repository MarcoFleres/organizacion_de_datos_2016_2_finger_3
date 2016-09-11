# coding=utf-8

import os
import sys
import graph

# Path for spark source folder
os.environ['SPARK_HOME']="/opt/hadoop/spark-1.6.2-bin-hadoop2.6"

# Append pyspark  to Python Path
sys.path.append("/opt/hadoop/spark-1.6.2-bin-hadoop2.6/python/")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print ("Successfully imported Spark Modules")

    graph.execute(SparkContext('local[*]'))

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)