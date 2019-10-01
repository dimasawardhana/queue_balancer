import sys
from pymongo import MongoClient
import multiprocessing as mp
import Queue

def multiThread():
    client = MongoClient('mongodb://localhost:27017/', maxPoolSize=10000)
    db = client.TA
    col = db.wordCount
    # newQ = []
    # for i in range(1000):
    data = {}
    #     "id": 1,
    #     "wordCount": [
    #         {
    #             "a" : 1,
    #             "b": 2
    #         }
    #     ]
    # }
        # newQ.append(data)
    x = col.insert_one(data)
    print x
    # print newQ
    # newQ[:] =[]
    # print newQ
    
    print "ok"
    

if __name__ == "__main__":
    pool = mp.Pool()
    workers = mp.cpu_count() - 1
    # for i in range(workers):
    #     print "initialize cpu",i
    #     pool.apply_async(multiThread)
    multiThread()
    try:
        while True:
            continue
    except KeyboardInterrupt:
        print ' [*] Exiting...'
        pool.terminate()
        pool.join()


