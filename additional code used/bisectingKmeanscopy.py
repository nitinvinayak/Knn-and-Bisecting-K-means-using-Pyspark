#importing Req libraries
from pyspark import *
import math
import copy
from statistics import mean
import os
from timeit import default_timer as timer

start = timer()
#Setting up Pyspark environment
appName="FDSassignment"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc=SparkContext(conf=conf)

#Defining constants
DATA_FILE='train2.txt'
#DATA_FILE="TCL10M13D"
K=3
MAX_ITERS=3
EPSILON=200

#Function Definitions
def addClusterArg(x):
    x.append(0)
    return x

def euclidean(x,y):
    distance=0
    for i in range(13):
        distance+=math.pow(int(x[i])-int(y[i]),2)
    return math.pow(distance,0.5)

def clusterAssignment(x,centroids):
    minDistance=euclidean(x,centroids[1])
    x[13]=1
    for i in range(2,len(centroids.keys())+1):
        if euclidean(x,centroids[i])<minDistance:
            x[13]=i
            minDistance=euclidean(x,centroids[i])
    return x

def clusterAssignment2(x,newCentroidDICT):
    minDistance=euclidean(x,newCentroidDICT[x[13]])
    #print(minDistance, end="")
    for i in newCentroidDICT.keys():
        if (i not in oldCentroidDICT.keys()):
            #print(newCentroidDICT.keys())
            #print(euclidean(x,newCentroidDICT[i]),"outside")
            if euclidean(x,newCentroidDICT[i])<=minDistance:
                x[13]=i
                minDistance=euclidean(x,newCentroidDICT[i])
                #print(minDistance,"inside")
    return x


def calculateCentroids():
    newCentroidDICT={}
    print(centroidDICT.keys())
    for i in centroidDICT.keys():
        newCentroidDICT[i]=[]
        tempRDD=dataRDD.filter(lambda x: x[13]==i)
        #print("DEBUG_______________",tempRDD.take(10))
        for j in range(len(centroidDICT[i])):
            tempRDD2=tempRDD.map(lambda x: x[j])
            newCentroidDICT[i].append(mean(tempRDD2.collect()))
    return newCentroidDICT

def calculateCentroids2():
    tempCentroidDICT={}
    print(newCentroidDICT.keys())
    for i in newCentroidDICT.keys():
        tempCentroidDICT[i]=[]
        tempRDD=dataRDD.filter(lambda x: x[13]==i)
        #print("DEBUG_______________",tempRDD.take(10))
        for j in range(len(newCentroidDICT[i])):
            tempRDD2=tempRDD.map(lambda x: x[j])
            tempCentroidDICT[i].append(mean(tempRDD2.collect()))
    return tempCentroidDICT

def assignNewKeys():
    newKeys=[]
    for key in range(1,len(newCentroidDICT.keys())+3):
        if key not in newCentroidDICT.keys():
            newKeys.append(key)
    return newKeys


def calculateError():
    maxError=0
    maxErrorIdx=0
    for i in centroidDICT.keys():
        tempRDD=dataRDD.filter(lambda x: x[13]==i)
        error=sum(tempRDD.map(lambda x: euclidean(x,centroidDICT[i])).collect())
        if error>maxError:
            maxError=error
            maxErrorIdx=i
    return maxErrorIdx

def calculateError2():
    maxError=0
    maxErrorIdx=0
    for i in newCentroidDICT.keys():
        tempRDD=dataRDD.filter(lambda x: x[13]==i)
        error=sum(tempRDD.map(lambda x: euclidean(x,newCentroidDICT[i])).collect())
        if error>maxError:
            maxError=error
            maxErrorIdx=i
    return maxErrorIdx

#loading data into RDD
dataRDD=sc.textFile(DATA_FILE)
dataRDD=dataRDD.map(lambda x:x.split("\t"))
dataRDD=dataRDD.filter(lambda x: len(x)==13)
dataRDD=dataRDD.map(lambda x: addClusterArg(x))
dataRDD=dataRDD.map(lambda x: list(map(int,x)))

#Initialising variables
centroidDICT=dict(zip([1,2],dataRDD.takeSample(False, 2, 4096)))

#Assigning initial clusters
dataRDD=dataRDD.map(lambda x: clusterAssignment(x,centroidDICT))

#Recalculating centroids
oldCentroidDICT=copy.deepcopy(centroidDICT)
centroidDICT=calculateCentroids()
dataRDD=dataRDD.map(lambda x: clusterAssignment(x,centroidDICT))

#Convergence of centroids
for j in centroidDICT.keys():
    iters=0
    print(euclidean(centroidDICT[j],oldCentroidDICT[j]))
    while (euclidean(centroidDICT[j],oldCentroidDICT[j])>EPSILON)and iters<MAX_ITERS:
        oldCentroidDICT=copy.deepcopy(centroidDICT)
        centroidDICT=calculateCentroids()
        dataRDD=dataRDD.map(lambda x: clusterAssignment(x,centroidDICT))
        iters+=1

clusterKey=calculateError()
newCentroidDICT=copy.deepcopy(centroidDICT)
del newCentroidDICT[clusterKey]

while(len(newCentroidDICT.keys())+1<K):
    newDataRDD=dataRDD.filter(lambda x: x[13]==clusterKey)
    otherDataRDD=dataRDD.filter(lambda x: x[13]!=clusterKey)
    newKeys=assignNewKeys()
    tempCentroidDICT=dict(zip(newKeys,newDataRDD.takeSample(False, 2, 2)))
    oldCentroidDICT=copy.deepcopy(newCentroidDICT)
    newCentroidDICT.update(tempCentroidDICT)
    print(len(newCentroidDICT.keys()))
    newDataRDD=newDataRDD.map(lambda x: clusterAssignment2(x,newCentroidDICT))
    dataRDD=newDataRDD.union(otherDataRDD)
    oldCentroidDICT=copy.deepcopy(newCentroidDICT)
    newDataRDD=newDataRDD.map(lambda x: clusterAssignment(x,newCentroidDICT))
    newCentroidDICT=calculateCentroids2()
    print(len(newCentroidDICT.keys()))
    for j in newCentroidDICT.keys():
        iters=0
        #print(euclidean(newCentroidDICT[j],oldCentroidDICT[j]))
        try:
            while (euclidean(newCentroidDICT[j],oldCentroidDICT[j])>EPSILON)and iters<MAX_ITERS:
                oldCentroidDICT=copy.deepcopy(newCentroidDICT)
                newCentroidDICT=calculateCentroids()
                newDataRDD=newDataRDD.map(lambda x: clusterAssignment(x,newCentroidDICT))
                iters+=1
        except:
            continue

    dataRDD=newDataRDD.union(otherDataRDD)
    clusterKey=calculateError2()
    del newCentroidDICT[clusterKey]
    print(len(oldCentroidDICT.keys()))


os.system('cls')
for i in range(1,K+1):
    print(dataRDD.filter(lambda x: x[13]==i).count(),"\t",i)


end = timer()
time_taken=(end - start)
print(time_taken,'seconds')
