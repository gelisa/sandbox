# run_spark.py

"""
a sandbox to test spark and learn how it works
to run it: put some (at least a couple) text files to the data folder
type in command line: python run_spark.py
or <path to spark home folder>/bin/spark-submit run_spark.py
to understand very basics of spark read: http://mlwhiz.com/blog/2015/09/07/Spark_Basics_Explained/
it is very good
"""

# this is to run with spark: python run_spark.py
from pyspark import SparkConf
from pyspark import SparkContext

# import python packages
import os
import numpy as np
# import my custom class
import custom_class

# set up spark environment
conf = SparkConf()
sc = SparkContext(conf=conf)

# text files shoud be here
folder = 'data/'
# init a list to put filenames in there to work with the files
a_list = []
for datafile in os.listdir(folder):
    a_list.append(os.path.join(folder,datafile))

# create a spark rdd from the list of file names
sp_filenames = sc.parallelize(a_list)
# now I use map to create a python class for every file (see custom_class.py for details)
sp_classes = sp_filenames.map(custom_class.Custom_class)
# now we apply a method function to every class.
# If this function creates or updates a class attribute it has to return an object itself
# so that spark has access to it (rdd's are immutable: you have to create a new rdd)
# we'll have [obj1, obj2, obj3 ...]
classes_wt_count = sp_classes.map(lambda x: x.count_lines())
# so no we want calculate some stats for every file.
# we use flatMap which returns seveal items of output for one item of inpet
# we'll have [stat1(dbj1), stat2(ojb1), stat1(obj2) ...]
dicts = classes_wt_count.flatMap(lambda x: x.get_stats())
# now instead of having a long list of key value pair we want to get [key1: list1, key2: list2 ]
# key is a name of stat and each list is a list of the stats for each object
dicts_collected = dicts.groupByKey().mapValues(list)
# now we calculate mean and standard deviation for every stat
stats = dicts_collected.map(lambda x: (x[0], np.mean(x[1]),np.std(x[1])))
# for spark to do the actual calculation we have to call an action
# for example collect()
print(stats.collect())