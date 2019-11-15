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

def euclidean_distance(x,y):
    distance=0
    for i in range(13):
        distance+=math.pow(int(x[i])-int(y[i]),2)
    return math.pow(distance,0.5)

def assign_clusters(x,centroids):
    minDistance=euclidean_distance(x,centroids[1])
    x[13]=1
    for i in range(2,len(centroids.keys())+1):
        if euclidean_distance(x,centroids[i])<minDistance:
            x[13]=i
            minDistance=euclidean_distance(x,centroids[i])
    return x

def assign_clusters_loop(x,currentMap):
    minDistance=euclidean_distance(x,currentMap[x[13]])
    #print(minDistance, end="")
    for i in currentMap.keys():
        if (i not in prevMap.keys()):
            #print(currentMap.keys())
            #print(euclidean_distance(x,currentMap[i]),"outside")
            if euclidean_distance(x,currentMap[i])<=minDistance:
                x[13]=i
                minDistance=euclidean_distance(x,currentMap[i])
                #print(minDistance,"inside")
    return x


def calculate_new_centroid():
    currentMap={}
    print(centroidMap.keys())
    for i in centroidMap.keys():
        currentMap[i]=[]
        tempRDD=dataRDD.filter(lambda x: x[13]==i)
        #print("DEBUG_______________",tempRDD.take(10))
        for j in range(len(centroidMap[i])):
            tempRDD2=tempRDD.map(lambda x: x[j])
            currentMap[i].append(mean(tempRDD2.collect()))
    return currentMap

def calculate_new_centroid2():
    tempCentroidDICT={}
    print(currentMap.keys())
    for i in currentMap.keys():
        tempCentroidDICT[i]=[]
        tempRDD=dataRDD.filter(lambda x: x[13]==i)
        #print("DEBUG_______________",tempRDD.take(10))
        for j in range(len(currentMap[i])):
            tempRDD2=tempRDD.map(lambda x: x[j])
            tempCentroidDICT[i].append(mean(tempRDD2.collect()))
    return tempCentroidDICT

def newkeys():
    newKeys=[]
    for key in range(1,len(currentMap.keys())+3):
        if key not in currentMap.keys():
            newKeys.append(key)
    return newKeys


def SSE():
    maxError=0
    maxErrorIdx=0
    for i in centroidMap.keys():
        tempRDD=dataRDD.filter(lambda x: x[13]==i)
        error=sum(tempRDD.map(lambda x: euclidean_distance(x,centroidMap[i])).collect())
        if error>maxError:
            maxError=error
            maxErrorIdx=i
    return maxErrorIdx

def Calc_error():
    maxError=0
    maxErrorIdx=0
    for i in currentMap.keys():
        tempRDD=dataRDD.filter(lambda x: x[13]==i)
        error=sum(tempRDD.map(lambda x: euclidean_distance(x,currentMap[i])).collect())
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
centroidMap=dict(zip([1,2],dataRDD.takeSample(False, 2, 4096)))

#Assigning initial clusters
dataRDD=dataRDD.map(lambda x: assign_clusters(x,centroidMap))

#Recalculating centroids
prevMap=copy.deepcopy(centroidMap)
centroidMap=calculate_new_centroid()
dataRDD=dataRDD.map(lambda x: assign_clusters(x,centroidMap))

#Convergence of centroids
for j in centroidMap.keys():
    iters=0
    print(euclidean_distance(centroidMap[j],prevMap[j]))
    while (euclidean_distance(centroidMap[j],prevMap[j])>EPSILON)and iters<MAX_ITERS:
        prevMap=copy.deepcopy(centroidMap)
        centroidMap=calculate_new_centroid()
        dataRDD=dataRDD.map(lambda x: assign_clusters(x,centroidMap))
        iters+=1

clusterKey=SSE()
currentMap=copy.deepcopy(centroidMap)
del currentMap[clusterKey]

while(len(currentMap.keys())+1<K):
    newRDD=dataRDD.filter(lambda x: x[13]==clusterKey)
    otherRDD=dataRDD.filter(lambda x: x[13]!=clusterKey)
    newKeys=newkeys()
    tempCentroidDICT=dict(zip(newKeys,newRDD.takeSample(False, 2, 2)))
    prevMap=copy.deepcopy(currentMap)
    currentMap.update(tempCentroidDICT)
    print(len(currentMap.keys()))
    newRDD=newRDD.map(lambda x: assign_clusters_loop(x,currentMap))
    dataRDD=newRDD.union(otherRDD)
    prevMap=copy.deepcopy(currentMap)
    newRDD=newRDD.map(lambda x: assign_clusters(x,currentMap))
    currentMap=calculate_new_centroid2()
    print(len(currentMap.keys()))
    for j in currentMap.keys():
        iters=0
        #print(euclidean_distance(currentMap[j],prevMap[j]))
        try:
            while (euclidean_distance(currentMap[j],prevMap[j])>EPSILON)and iters<MAX_ITERS:
                prevMap=copy.deepcopy(currentMap)
                currentMap=calculate_new_centroid()
                newRDD=newRDD.map(lambda x: assign_clusters(x,currentMap))
                iters+=1
        except:
            continue

    dataRDD=newRDD.union(otherRDD)
    clusterKey=Calc_error()
    del currentMap[clusterKey]
    print(len(prevMap.keys()))


os.system('cls')
for i in range(1,K+1):
    print(dataRDD.filter(lambda x: x[13]==i).count(),"\t",i)


end = timer()
time_taken=(end - start)
print(time_taken,'seconds')
