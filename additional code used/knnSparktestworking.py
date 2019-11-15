from pyspark import *
import math
import operator
import random
appName="knnSpark"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
k=1000
def euclidean(x,test):
    distance=0
    for i in range(9):
        distance+=math.pow(int(x[i])-int(test[i]),2)
    return [math.pow(distance,0.5),x[9]]
n=5
def count(x,d):
    for i in range(len(x)):
        if int(list(x[i])[1]) not in d.keys():
            d[int(list(x[i])[1])]=1
        else:
            d[int(list(x[i])[1])]+=1
    print( max(d.items(), key=operator.itemgetter(1)))
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
        if str(classifiedas)==str(RDDtestList[i][9]):
            accuracy+=1
    return(accuracy*100.0/n)


def DataSplit(x):
    split=0.8  
    if random.random()<split:
        y=x+(" train")
    else:
        y=x+(" test")
    return y

'''def TrainData(x):
    if x.split()[-1]=="train":
        return True
    else:
        return False
''' 
sc=SparkContext(conf=conf)
RDDList=sc.textFile("shuttle.tst")
RDDSplit=RDDList.map(lambda x: DataSplit(x))
RDDList2=RDDSplit.map(lambda x: x.split())
RDDtrain=RDDList2.filter(lambda x: x[-1]=="train")
RDDtest=RDDList2.filter(lambda x: x[-1]=="test")
RDDtestlength=int(RDDtest.count())
RDDtestList=RDDtest.take(RDDtestlength)
print(testknn(5))
print("___________________DONE SUCCESSFULLY__________________")

