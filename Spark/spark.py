# Databricks notebook source

#2.1 First RDD
l = list(range(0,3000))
rdd = sc.parallelize(l)

# 2.2 Computing the sum of cubes

C = rdd.map(lambda a : a*a*a)
C.sum()

# 2.3 Last digits of elements in C

lastDig = C.map(lambda v : (v%10, 1))
totalLastDig = lastDig.reduceByKey(lambda a,b : a+b).sortByKey()
totalLastDig.collect()

# 2.4 Digits of C

def digits(i):
  return [(e,1) for e in str(i)]

dig = C.flatMap(lambda v: digits(v))
digTotal = dig.reduceByKey(lambda a,b : a+b).sortByKey()
digTotal.collect()

# 3.1 Step 1 : computing set of pairs

pairs = rdd.cartesian(rdd)

# 3.2 Step 2 : counting the pairs

filterPairs = pairs.filter(lambda p :(2*p[0]+1)*(2*p[0]+1)+(2*p[1]+1)*(2*p[1]+1) < (2*3000)*(2*3000)).count()

# 3.3 Computing the approximation

print(filterPairs/(pairs.count())*4)

# 4.2 Getting the dataset into an RDD

import re
future_pattern = re.compile("""([^,"]+|"[^"]+")(?=,|$)""")
def parseCSV(line):
  return future_pattern.findall(line)
path_data = "/FileStore/tables/"
ratingsFile = sc.textFile(path_data+"/ratings.csv").map(parseCSV)
moviesFile = sc.textFile(path_data+"/movies.csv").map(parseCSV)

# 4.3 Cleaning data

rating = ratingsFile.filter(lambda r : r[0] != 'userId')
r = rating.map(lambda r : (r[0],r[1],float(r[2]),r[3]))
movie = moviesFile.filter(lambda m : m[0] != 'movieId')

# 4.4 10 best movies of all times

best10List = r.map(lambda r : (r[0], r[2])).mapValues(lambda f : (f,1)).reduceByKey(lambda a,b : (a[0]+b[0], a[1]+b[1])).mapValues(lambda x : (x[0]/x[1])).sortBy((lambda x : x[1]), False).take(10)

# 4.5 Ordered list of movies with names

best10Rdd = sc.parallelize(best10List)
best10Rdd.join(movie).values().collect()

# 4.6 Better ordered list

grade1 = r.map(lambda r : (r[0], r[2])).mapValues(lambda f : (f,1)).reduceByKey(lambda a,b : (a[0]+b[0], a[1]+b[1])).mapValues(lambda x : (x[0]/x[1]*(x[1]/(x[1]+1)))).sortBy((lambda x : x[1]), False).take(10)
best10_1 = sc.parallelize(grade1)
best10_1.join(movie).values().collect()

# COMMAND ----------

import math
grade2 = r.map(lambda r : (r[0], r[2])).mapValues(lambda f : (f,1)).reduceByKey(lambda a,b : (a[0]+b[0], a[1]+b[1])).mapValues(lambda x : (x[0]/x[1]*(math.log(x[1])))).sortBy((lambda x : x[1]), False).take(10)
best10_2 = sc.parallelize(grade2)
best10_2.join(movie).values().collect()



