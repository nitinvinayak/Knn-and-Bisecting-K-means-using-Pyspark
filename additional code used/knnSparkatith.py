from pyspark import *
import math
import operator
import random
from timeit import default_timer as timer

start = timer()

k=10
n=5

appName="knn_spark"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)

def euclidean(x,test):
    distance=0
    for i in range(9):
        distance+=math.pow(int(x[i])-int(test[i]),2)
    return [math.pow(distance,0.5),x[9]]

def count(x,d):
    for i in range(len(x)):
        if int(list(x[i])[1]) not in d.keys():
            d[int(list(x[i])[1])]=1
        else:
            d[int(list(x[i])[1])]+=1
    #print( max(d.items(), key=operator.itemgetter(1)))
    #print(d.items())
    #print( max(d.items(), key=operator.itemgetter(1)))
    #print( max(d.items(), key=operator.itemgetter(1))[0])
    # print(max(d.items(), key=operator.itemgetter(1))[0])
    v=list(d.values())
    k=list(d.keys())
    return k[v.index(max(v))]
    # return max(d.items(), key=operator.itemgetter(1))[0]

def testknn(n):
    accuracy=0
    for i in range(n):
        d={}
        # testlist=test_list[i]
        Dist_RDD=preprocessed_train.map(lambda x: euclidean(x,test_list[i]))
        classifiedas=count(Dist_RDD.takeOrdered(k, key = lambda x: x[0]),d)
        #print(((classifiedas.split()[-1]).rstrip("]"))[1:-1],test_list[i][9])
        #print(len(d.keys()))
        if str(classifiedas) == str(test_list[i][9]):
            accuracy+=1
    return(accuracy*100.0/n)

sc=SparkContext(conf=conf)
test_data=sc.textFile("shuttle.tst")
train_data=sc.textFile("shuttle.tst")
#RDDSplit=RDDList.map(lambda x: DataSplit(x))
preprocessed_test=test_data.map(lambda x: x.split())
preprocessed_train=train_data.map(lambda x: x.split())
test_length=int(preprocessed_test.count())
test_list=preprocessed_test.take(test_length)
accuracyPercent=testknn(n)
end = timer()
time_taken=(end - start)
print("ACCURACY=",accuracyPercent,"in",time_taken, "SECONDS")
print("___________________DONE SUCCESSFULLY__________________")

