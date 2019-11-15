from pyspark import *
import math
from statistics import mean
from timeit import default_timer as timer

start = timer()

appName="FDSassignment_BisectingKmeans"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc=SparkContext(conf=conf)

k=5
ck=0
Epsilon=4

def newCentroids():
    newCentroid=[[0 for _ in range(13)] for _ in range(ck+2)]
    #clusterList=[0 for _ in range(k)]
    for clusterNum in range(ck,ck+2):
        for i in range(0,13):
            #print(RDDclustered2.map(lambda x: x[i]).take(int(RDDclustered2.map(lambda x: x[i]).count())))
            tempRDD=RDDclustered2.map(lambda x: x[i])
            newCentroid[clusterNum][i]=(mean(map(int,list(tempRDD.take(int(tempRDD.count()))))))
        newCentroid[clusterNum].append(clusterNum)
    return newCentroid

def euclidean(x,centroid):
    distance=0
    for i in range(len(x)-1):
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

def initialise2Centroids(rdd):
    l=rdd.takeSample(False, 2, 1)
    for i in range(2):
        l[i][13]=i+ck+1
    return l

def calculateLSE(x,centroid,LSE):
    LSE+=euclidean(x,centroid)
    return LSE

RDDList=sc.textFile("TCL10M13D")
RDDList2=RDDList.map(lambda x: x.split("\t"))
RDDfiltered=RDDList2.filter(lambda x: len(x)==13)
RDDclustered=RDDfiltered.map(lambda x: addClusterArg(x))
Centroids= initialise2Centroids(RDDclustered)
RDDclustered2=RDDclustered.map(lambda x: clusterAssignment(x,Centroids))
newCentroid=[[] for _ in range(k)]
newCentroid=newCentroids()
RDDclustered2=RDDclustered2.map(lambda x: clusterAssignment(x,newCentroid))

for i in range(ck,ck+2):
    #os.system("clear")
    #input()
    #print(i)
    #print((newCentroid))
    #print((Centroids))
    dist=euclidean(newCentroid[i],Centroids[i])
    #print(dist)
    while(dist>Epsilon):
        Centroids=newCentroid
        newCentroid=newCentroids()
        RDDclustered2=RDDclustered2.map(lambda x: clusterAssignment(x,newCentroid))
        dist=euclidean(newCentroid[i],Centroids[i])
        print(dist)
LSElist=[0 for _ in range(ck+2)]
for i in range(ck,ck+2):
    LSElist[i]=sum(map(int,((RDDclustered2.filter(lambda x: x[13]==i).map(lambda x: euclidean(x,Centroids[i]))).collect())))
newdatasetidx=LSElist.index(max(LSElist))
print(newdatasetidx)
ck+=1
#ck+=2

RDDcluster=RDDclustered2.filter(lambda x: x[13]==newdatasetidx).map(lambda x: euclidean(x,Centroids[newdatasetidx]))
RDDcluster.take(10)
while(ck<k):
    Centroids= initialise2Centroids(RDDclustered)
    RDDclustered2=RDDclustered.map(lambda x: clusterAssignment(x,Centroids))
    newCentroid=[[] for _ in range(k)]
    newCentroid=newCentroids()
    RDDclustered2=RDDclustered2.map(lambda x: clusterAssignment(x,Centroids))
    for i in range(ck,ck+2):
        #os.system("clear")
        #input()
        #print(i)
        print((newCentroid))
        print((Centroids))
        dist=euclidean(newCentroid[i],Centroids[i])
        print(dist)
        while(dist>Epsilon):
            Centroids=newCentroid
            newCentroid=newCentroids()
            RDDcluster=RDDcluster.map(lambda x: clusterAssignment(x,Centroids))
            dist=euclidean(newCentroid[i],Centroids[i])
            print(dist)
    LSElist=[0 for _ in range(ck+2)]
    for i in range(ck,ck+2):
        LSElist[i]=sum(map(int,((RDDcluster.filter(lambda x: x[13]==i).map(lambda x: euclidean(x,Centroids[i]))).collect())))
    newdatasetidx=LSElist.index(max(LSElist))
    print(newdatasetidx)
    ck+=1
    #ck+=2
    RDDcluster=RDDclustered2.filter(lambda x: x[13]==newdatasetidx).map(lambda x: euclidean(x,Centroids[newdatasetidx]))
    #RDDclustered2=
end = timer()
time_taken=(end - start)

print("time_taken",SECONDS)
print("___________________DONE SUCCESSFULLY__________________")
