from pyspark import *
import math
import operator
import random

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
    print(max(d.items(), key=operator.itemgetter(1))[0])
    return max(d.items(), key=operator.itemgetter(1))[0]

def testknn(n):
    accuracy=0
    for i in range(n):
        d={}
        testlist=testList[i]
        tempRDD=RDDtrain.map(lambda x: euclidean(x,testlist))
        classifiedas=count(tempRDD.takeOrdered(k, key = lambda x: x[0]),d)
        #print(((classifiedas.split()[-1]).rstrip("]"))[1:-1],testList[i][9])
        if str(classifiedas)==str(testList[i][9]):
            accuracy+=1
    return(accuracy*100.0/n)

appName="knnSpark"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc=SparkContext(conf=conf)

k=1000
n=5

RDDtrain=sc.textFile("shuttle.trn")
RDDtest=sc.textFile("shuttle.tst")

RDDtrain2=RDDtrain.map(lambda x: x.split())
RDDtest2=RDDtest.map(lambda x: x.split())

testlength=int(RDDtest2.count())
testList=RDDtest2.take(testlength)

print(testknn(5))
print("___________________DONE SUCCESSFULLY__________________")

