# spark-basic.py
from pyspark import SparkConf
from pyspark import SparkContext
import os
import simple_app

conf = SparkConf()
sc = SparkContext(conf=conf)

folder = '../research/bagrow/datastorage/BA_rte_data/'
a_list = []
for datafile in os.listdir(folder):
    a_list.append(os.path.join(folder,datafile))

sp_filenames = sc.parallelize(a_list)
sp_classes = sp_filenames.map(simple_app.Custom_class)
line_counts = sp_classes.map(lambda x: x.count_lines())
print(line_counts.reduce(lambda x,y: x+y))
