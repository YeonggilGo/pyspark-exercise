tranFile = sc.textFile("path/to/ch04_data_transactions.txt")
tranData = tranFile.map(lambda line: line.split("#"))
transByCust = tranData.map(lambda t: (int(t[2]), t))

transByCust.keys().distinct().count()

import operator

transByCust.countByKey()
sum(transByCust.countByKey().values())
(cid, purch) = sorted(transByCust.countByKey().items(), key=operator.itemgetter(1))[-1]
complTrans = [["2015-03-30", "11:59 PM", "53", "4", "1", "0.00"]]

transByCust.lookup(53)
for t in transByCust.lookup(53):
    print(", ".join(t))


def applyDiscount(tran):
    if (int(tran[3]) == 25 and float(tran[4]) > 1):
        tran[5] = str(float(tran[5]) * 0.95)
    return tran


transByCust = transByCust.mapValues(lambda t: applyDiscount(t))


def addToothbrush(tran):
    if (int(tran[3]) == 81 and int(tran[4]) > 4):
        from copy import copy
        cloned = copy(tran)
        cloned[5] = "0.00"
        cloned[3] = "70"
        cloned[4] = "1"
        return [tran, cloned]
    else:
        return [tran]


transByCust = transByCust.flatMapValues(lambda t: addToothbrush(t))

amounts = transByCust.mapValues(lambda t: float(t[5]))
totals = amounts.foldByKey(0, lambda p1, p2: p1 + p2).collect()
amounts.foldByKey(100000, lambda p1, p2: p1 + p2).collect()

complTrans += [["2015-03-30", "11:59 PM", "76", "63", "1", "0.00"]]
transByCust = transByCust.union(sc.parallelize(complTrans).map(lambda t: (int(t[2]), t)))
transByCust.map(lambda t: "#".join(t[1])).saveAsTextFile("/destination")

prods = transByCust.aggregateByKey([], lambda prods, tran: prods + [tran[3]],
                                   lambda prods1, prods2: prods1 + prods2)
prods.collect()
