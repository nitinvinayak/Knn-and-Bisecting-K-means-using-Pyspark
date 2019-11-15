from pyspark import *
import math
import operator
import random
from timeit import default_timer as timer

start = timer()

k=4
n=400

appName="knnSpark"
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
    print(max(d.items(), key=operator.itemgetter(1))[0])
    return max(d.items(), key=operator.itemgetter(1))[0]

def testknn(n):
    accuracy=0
    for i in range(n):
        d={}
        testlist=RDDtestList[i]
        tempRDD=RDDtrain.map(lambda x: euclidean(x,testlist))
        classifiedas=count(tempRDD.takeOrdered(k, key = lambda x: x[0]),d)
        #print(((classifiedas.split()[-1]).rstrip("]"))[1:-1],RDDtestList[i][9])
        #print(len(d.keys()))
        if str(classifiedas)==str(RDDtestList[i][9]):
            accuracy+=1
    return(accuracy*100.0/n)

sc=SparkContext(conf=conf)
RDDtest3=sc.textFile("shuttle.tst")
RDDtrain3=sc.textFile("shuttle.tst")
#RDDSplit=RDDList.map(lambda x: DataSplit(x))
RDDtest=RDDtest3.map(lambda x: x.split())
RDDtrain=RDDtrain3.map(lambda x: x.split())
RDDtestlength=int(RDDtest.count())
RDDtestList=RDDtest.take(RDDtestlength)
accuracyPercent=testknn(n)
end = timer()
time_taken=(end - start)
print("ACCURACY=",accuracyPercent,"in",time_taken, "SECONDS")
print("___________________DONE SUCCESSFULLY__________________")

