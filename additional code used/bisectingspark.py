from pyspark import *
import math
import operator
from statistics import mean

appName="FDSassignment"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)

test=[1,1,1,1,1,1,1,1,1,1,1,1,1]
k=4
ck=0

def euclidean(x,centroid):
    distance=0
    for i in range(13):
        distance+=math.pow(int(x[i])-int(centroid[i]),2)
    return math.pow(distance,0.5)

def clusterAssignment(x,centroids):
    minDistance=euclidean(x,centroids[0])
    x[13]=centroids[0][13]
    for i in range(1,len(centroids)):
        if euclidean(x,centroids[i])<minDistance:
            x[13]=centroids[i][13]
            minDistance=euclidean(x,centroids[i])
    return x
            
def addClusterArg(x):
    x.append(None)
    return x
        
'''def count(x):
    d={}
    if str(x[1]) not in d.keys():
        d[str(x[1])]=1
    else:
        d[str(x[1])]+=1
    return max(d.items(), key=operator.itemgetter(1))[0]
'''
#def calculateCentroid():

def newCentroids():
    newCentroid=[[] for _ in range(ck)]
    #clusterList=[0 for _ in range(k)]
    for clusterNum in range(ck):
        for i in range(0,13):
            #print(RDDclustered2.map(lambda x: x[i]).take(int(RDDclustered2.map(lambda x: x[i]).count())))
            tempRDD=RDDclustered2.map(lambda x: x[i])
            newCentroid[clusterNum].append(mean(map(int,list(tempRDD.take(int(tempRDD.count()))))))
    return newCentroid
    
    '''
    newcentroid=rdd.map(calculateCentroid(x))
    clusterIdx=clusterNum-1
    updatedCentroids[clusterIdx]=[int(a)+int(b) for a,b in zip(updatedCentroids[clusterIdx],x)]
    numberOfpoints[clusterIdx]+=1
    x[13]=None
    '''
def initialise2Centroids(rdd):
    l=rdd.takeSample(False, 2, 1)
    for i in range(2):
        l[i][13]=i+ck+1
    return l
        
    
sc=SparkContext(conf=conf)
RDDList=sc.textFile("train.txt")
RDDList.take(10)
#RDDList12 = sc.parallelize(RDDList.take(10000002))
RDDList2=RDDList.map(lambda x: x.split("\t"))
RDDList2.take(10)
RDDfiltered=RDDList2.filter(lambda x: len(x)==13)
#RDDfiltered=RDDList.filter(lambda x: returnLists(x))
RDDfiltered.take(10)
RDDclustered=RDDfiltered.map(lambda x: addClusterArg(x))
Centroids= initialise2Centroids(RDDclustered)
ck+=2
RDDclustered2=RDDclustered.map(lambda x: clusterAssignment(x,Centroids))
newCentroid=[[] for _ in range(k)]
clusterList=[0 for _ in range(k)]
'''
for clusterNum in range(ck):
    for i in range(0,13):
        #print(RDDclustered2.map(lambda x: x[i]).take(int(RDDclustered2.map(lambda x: x[i]).count())))
        tempRDD=RDDclustered2.map(lambda x: x[i])
        newCentroid[clusterNum].append(mean(map(int,list(tempRDD.take(int(tempRDD.count()))))))
'''
newCentroid=newCentroids()
RDDclustered2=RDDclustered2.map(lambda x: clusterAssignment(x,newCentroid))
#updatedCentroids=[[0 for _ in range(14)] for i in range(1,ck+1)]
#numberOfpoints=[0 for _ in range(1,ck+1)]
#RDDclustered2=RDDclustered2.map(lambda x: newCentroids(x))
'''
for i in range(1,ck+1):
    centroids[i]=[a/b for a,b in zip(updatedCentroids[i],numberOfpoints)]
'''
clusterList=[[] for _ in range(k)]
newCentroid=[[] for _ in range(ck)]
#clusterList=[0 for _ in range(k)]
for clusterNum in range(ck):
    for i in range(0,13):
        #print(RDDclustered2.map(lambda x: x[i]).take(int(RDDclustered2.map(lambda x: x[i]).count())))
        tempRDD=RDDclustered2.map(lambda x: x[i])
        newCentroid[clusterNum].append(mean(map(int,list(tempRDD.take(int(tempRDD.count()))))))
#print(newCentroid)
#clusterList=RDDclustered2.map(lambda x: x[13]).take(10000000)
'''
for i in range(k):
    clusterList[i]=RDDclustered2.filter(lambda (key,index) : index if int(key[13])==i)
for i in range(ck):
    clusterList[i]=RDDclustered2.map(lambda x: x[13]).take(10000000)
    print(RDDclusters[i].count())
'''
#RDDdistance=RDDfiltered.map(lambda x: euclidean(x,test))
#Listordered=RDDdistance.takeOrdered(k, key = lambda x: x[0])
#print(count(Listordered))
#print(RDDfiltered.take(10))
#print(RDDfiltered.count())
print("___________________DONE SUCCESSFULLY__________________")


