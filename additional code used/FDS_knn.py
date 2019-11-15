from pyspark import *
import math
import operator
import random
from timeit import default_timer as timer
import os

start = timer()

k=4
n=30

appName="FDSAssignment_knnSpark"
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
    matched=0
    for i in range(n):
        d={}
        testlist=testList[i]
        tempRDD=RDDtrain2.map(lambda x: euclidean(x,testlist))
        classifiedas=count(tempRDD.takeOrdered(k, key = lambda x: x[0]),d)
        #print(((classifiedas.split()[-1]).rstrip("]"))[1:-1],RDDtestList[i][9])
        #print(len(d.keys()))
        if str(classifiedas)==str(testList[i][9]):
            matched+=1
    return(matched*100.0/n)

sc=SparkContext(conf=conf)
RDDtest1=sc.textFile("shuttle.tst")
RDDtrain1=sc.textFile("shuttle.trn")
#RDDSplit=RDDList.map(lambda x: DataSplit(x))
RDDtest2=RDDtest1.map(lambda x: x.split())
RDDtrain2=RDDtrain1.map(lambda x: x.split())
testlength=int(RDDtest2.count())
testList=RDDtest2.take(testlength)
accuracyPercent=testknn(n)
end = timer()
time_taken=(end - start)
os.system("cls")
FILE_NAME="outputKnn_k"+str(k)+"_n"+str(n)+".txt"
ofile=open(FILE_NAME,"w")
ofile.write(("ACCURACY = "+str(accuracyPercent)+" in "+str(time_taken)+ " SECONDS FOR "+str(k)+" neighbours and "+str(n)+" testcases"))
ofile.close()
print("___________________DONE SUCCESSFULLY__________________")
