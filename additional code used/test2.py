from pyspark import *
import math
import operator
import random
from timeit import default_timer as timer
from collections import defaultdict
start = timer()

k=10
n=5

def most_frequent_class(x,d):
    for i in range(len(x)):
        #if int(list(x[i])[0]) not in d.keys():
            #d[int(list(x[i])[0])]=1
        #else:
        d[int(list(x[i])[0])]+=1
    v=list(d.values())
    k=list(d.keys())
    return k[v.index(max(v))]

def dist(x,test):
    distance=0
    for i in range(9):
        distance+=math.pow(int(x[i])-int(test[i]),2)
    return [x[9],math.sqrt(distance)]

appName="knn_spark"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)

sc=SparkContext(conf=conf)
test_data=sc.textFile("shuttle.tst")
train_data=sc.textFile("shuttle.tst")
preprocessed_test=test_data.map(lambda x: x.split())
preprocessed_train=train_data.map(lambda x: x.split())
test_list=preprocessed_test.collect()
correst_predict_no=0
for i in range(n):
    d={}
    d=defaultdict(int)
    Dist_RDD=preprocessed_train.map(lambda x: dist(x,test_list[i]))
    class_no=most_frequent_class(Dist_RDD.takeOrdered(k, key = lambda x: x[1]),d)
    if str(class_no) == str(test_list[i][9]):
    	correst_predict_no+=1
   
acc_rate=correst_predict_no*100.0/n
end = timer()
time_taken=(end - start)
print("ACCURACY=",acc_rate,"in",time_taken, "SECONDS")



    
