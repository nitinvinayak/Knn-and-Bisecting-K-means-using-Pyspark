from pyspark import *
import math
import operator

appName="FDSassignment"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
test=[1,1,1,1,1,1,1,1,1,1,1,1,1]
k=100
def returnLists(x):
    if len(x)==13:
        return True
    else:
        return False
def euclidean(x,test):
    distance=0
    for i in [0,1,2,3,4,5,6,7,8,10,11,12]:
        distance+=math.pow(int(x[i])-test[i],2)
    return [distance,x[9]]

def count(x):
    d={}
    if str(x[1]) not in d.keys():
        d[str(x[1])]=1
    else:
        d[str(x[1])]+=1
    return max(d.items(), key=operator.itemgetter(1))[0]
sc=SparkContext(conf=conf)
RDDList=sc.textFile("TCL10M13D",3000)
RDDList.take(10)
#RDDList12 = sc.parallelize(RDDList.take(10000002))
    
RDDList2=RDDList.map(lambda x: x.split("\t"))
RDDList2.take(10)
RDDfiltered=RDDList2.filter(lambda x: len(x)==13)
#RDDfiltered=RDDList.filter(lambda x: returnLists(x))
RDDfiltered.take(10)
RDDdistance=RDDfiltered.map(lambda x: euclidean(x,test))
Listordered=RDDdistance.takeOrdered(k, key = lambda x: x[0])
print(count(Listordered))
print(RDDfiltered.take(10))
print(RDDfiltered.count())
print("___________________DONE SUCCESSFULLY__________________")


