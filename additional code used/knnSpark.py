from pyspark import *
import math
import operator
import random
appName="knnSpark"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)

def euclidean(x,test):
    distance=0
    for i in range(9):
        distance+=math.pow(int(x[i])-int(test[i]),2)
    return [math.pow(distance,0.5),x[9]]

def count(x):
    d={}
    if str(x[1]) not in d.keys():
        d[str(x[1])]=1
    else:
        d[str(x[1])]+=1
    print( max(d.items(), key=operator.itemgetter(1))[0].split()[1])
    return max(d.items(), key=operator.itemgetter(1))[0].split()[1]

def testknn(n):
    accuracy=0
    for i in range(n):
        testlist=RDDtestList[i]
        tempRDD=RDDtrain.map(lambda x: euclidean(x,testlist))
        classifiedas=count(tempRDD.takeOrdered(k, key = lambda x: x[0]))
        if str(classifiedas[1])==str(RDDtestList[i][9]):
            print(classifiedas,RDDtestList[i][9])
            accuracy+=1
    return accuracy*100.0/n

k=1000
n=5

sc=SparkContext(conf=conf)
RDDtrain=sc.textFile("shuttle.trn")
RDDtest=sc.textFile("shuttle.tst")

RDDtrain2=RDDtrain.map(lambda x: x.split())
RDDtest2=RDDtest.map(lambda x: x.split())

RDDtestlength=int(RDDtest2.count())
RDDtestList=RDDtest2.take(RDDtestlength)

print(testknn(5))
print("___________________DONE SUCCESSFULLY__________________")

