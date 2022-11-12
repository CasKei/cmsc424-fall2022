import json
import re

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
    global dummyrdd
    dummyrdd = rdd

# ---------------------------------
def task1(postsRDD):
    res = postsRDD.filter(
        lambda x: x.get("tags") != None).filter(
        lambda x: "postgresql-9.4" in x.get("tags")).map(
        lambda x: (x.get("id"), x.get("title"), x.get("tags"))
    )
    return res

#------------------------------------
def help2(d):
    res = []
    tags = d.get("tags").replace("<", "").split(">")
    tags.pop()
    for i in tags:
        res.append( (d.get("id"), i) )
    return res
    

def task2(postsRDD):
    res = postsRDD.filter(lambda x: x.get("tags") != None).flatMap(help2)
    return res

# ------------------------------------
def help3_1(d):
    tags = d.get("tags").replace("<", "").split(">")
    tags.pop()
    yr = d['creationdate'][:4] # date: 2011-02-02 format, so get year by splice
    return (yr, set(tags))

def help3_2(tup):
    tags = tup[1]
    tags = list(tags)
    tags.sort()
    return (tup[0], tags[:5])

def task3(postsRDD):
    res = postsRDD.filter(lambda x: x.get('tags') != None).map(help3_1).reduceByKey(lambda a,b: a | b).map(help3_2)
    return res

#------------------------------------
def combine(tup):
    a = tup[0]
    b = tup[1][0]
    c = tup[1][1][0]
    d = tup[1][1][1]
    return (a,b,c,d)

def task4(usersRDD, postsRDD):
    user = usersRDD.map(lambda x: (x.get("id"),  x.get("displayname")))
    post = postsRDD.map(lambda x: (x.get('owneruserid') , (x.get('id'), x.get('title'))))
    res = user.join(post)
    return res.map(combine)

#----------------------------------
def help5(d):
    res = []
    tags = d.get("tags").replace("<", "").split(">")
    tags.pop()
    title = d.get('title').split(' ')
    for i in tags:
        for t in title:
            res.append( (t,i) )
    return res

# RDD.aggregateByKey(zeroValue, seqFunc, combFunc, numPartitions=None, partitionFunc=<function portable_hash>)

def task5(postsRDD):
    key = postsRDD.filter(lambda x: x.get("tags") != None).flatMap(help5)
    tup = key.map(lambda x : (x, 0))
    res = tup.aggregateByKey(1, lambda x,y:x+y , lambda x,y: x+y)
    return res

#------------------------------------
def rem(s):
    l = []
    l = s.replace('user', '').replace('product', '').split(' ')
    return (l[0], l[1], l[2])
    
def task6(amazonInputRDD):
    res = amazonInputRDD.map(rem)
    return res

#--------------------------------------
def task7(amazonInputRDD):
    return dummyrdd

def task8(amazonInputRDD):
    return dummyrdd

def task9(logsRDD):
    return dummyrdd

def task10_flatmap(line):
    return line

def task11(playRDD):
    return dummyrdd

def task12(nobelRDD):
    return dummyrdd

def task13(logsRDD, l):
    return dummyrdd

def task14(logsRDD, day1, day2):
    return dummyrdd

def task15(bipartiteGraphRDD):
    return dummyrdd

def task16(nobelRDD):
    return dummyrdd
