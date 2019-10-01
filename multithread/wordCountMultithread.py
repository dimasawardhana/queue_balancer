import concurrent.futures
import random
import time
import pika
from ast import literal_eval
import os
import sys
import logging
import Queue
from pymongo import MongoClient
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer, PorterStemmer
import threading 
import logging
import multiprocessing as mp

class Pipeline:
    """
    Class to allow a single element pipeline between producer and consumer.
    """
    def __init__(self):
        self.message = 0
        #
        self.msg = ""
        self.stemmer = PorterStemmer()
        self.toLowercase = ""
        self.toToken = ""
        self.toStopWord = []
        self.toStem = []
        self.resultData = []
        self._id = 1000
        self.queue = sys.argv[2]
        self.prefetch = int(sys.argv[1])
        self.stop_words = set(stopwords.words("english"))
        self.prod_lock = threading.Lock()
        self.lowercase_lock = threading.Lock()
        self.tokenizing_lock = threading.Lock()
        self.stopword_lock = threading.Lock()
        self.stem_lock = threading.Lock()
        self.wordCount_lock = threading.Lock()
        
        self.lowercase_lock.acquire()
        self.tokenizing_lock.acquire()
        self.stopword_lock.acquire()
        self.stem_lock.acquire()
        self.wordCount_lock.acquire()
        #
        self.count = 0
        self.datax = []
        self.status = True
        self.connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()
        # mongodb
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client.TA
        self.collection = self.db.wordCount
        self.docQueue = Queue.Queue()
        
    def produce(self):
        self.channel.queue_declare(queue=self.queue,durable=True)
        self.channel.basic_qos(prefetch_count=self.prefetch)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=False)
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        dataToProcess = literal_eval(body)
        data = dataToProcess
        self.msg = data["content"]
        self._id = data["id"]
        self.prod_lock.acquire()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self.lowercase_lock.release()    

    def lowercasing(self):
        self.lowercase_lock.acquire()
        logging.info("lowercasing text on id %s",self._id)
        self.toLowercase = self.msg.lower()    
        self.tokenizing_lock.release()
        logging.info("data %s ready to tokenize", self._id)

    def tokenize(self):
        self.tokenizing_lock.acquire()
        logging.info("tokenize text on id %s", self._id)
        tokenizer = RegexpTokenizer(r'(?u)\b[\w\-\'\.\/\\\:]+\w\b')
        self.toToken = tokenizer.tokenize(self.toLowercase)
        self.stopword_lock.release()

    def sw_removal(self):
        #removal
        self.stopword_lock.acquire()
        logging.info("remove stopword on id %s", self._id)
        self.toStopWord = [w for w in self.toToken if not w in self.stop_words]
        self.stem_lock.release()
        
    def stem(self):
        
        self.stem_lock.acquire()
        self.toStem = [self.stemmer.stem(w) for w in self.toStopWord]
        # logging.info("hasil stemming : %s",self.toStem)
        self.wordCount_lock.release()

    def result(self):
        self.wordCount_lock.acquire()
        logging.info("wordCount on id %s", self._id)
        list_tf = {}

        for w in self.toStem:
            if w not in list_tf:
                list_tf[w] = 0
            list_tf[w] += 1
        data  = {
            "id" : self._id,
            "result" : list_tf
        }

        self.docQueue.put(data)
        self.prod_lock.release()
        
        return data
    def insertDoc(self):
        if not self.docQueue.empty():
            # self.datax.append(self.docQueue.get())
            # print len(self.datax)
            # if len(self.datax) > 100:
            #     x = self.collection.insert_many(self.datax).inserted_ids              
            #     self.datax[:] = []
            #     print x
            data = self.docQueue.get()
            x = self.collection.insert_one(data).inserted_id
            print x
        
    def hasDone(self):
        self.status = False
    def getStatus(self):
        return self.status
    
        
        # pipeline.hasDone()

#main program
def _produce(pipeline):
    pipeline.produce()

def _lowercase(pipeline):
    while pipeline.getStatus():
        pipeline.lowercasing()
def _tokenize(pipeline):
    while pipeline.getStatus():
        pipeline.tokenize()
def _stopword(pipeline):
    while pipeline.getStatus():
        pipeline.sw_removal()
def _stem(pipeline):
    while pipeline.getStatus():
        pipeline.stem() 
        # print type(pipeline.queue), type(pipeline.prefetch)
def _result(pipeline):
    while pipeline.getStatus():
        mes = pipeline.result()
        # print mes
        # logging.debug("get Result on id %d, : %s", mes["id"], mes["result"])
        # success = col.insert_one(mes).inserted_id
        # logging.info("%d inserted", success)
def _insert(pipeline):
    while pipeline.getStatus():
        pipeline.insertDoc()

def multiThread():
    print "ready pool # ", mp.current_process().name, "\n"
    pipeline = Pipeline()
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        executor.submit(_produce, pipeline)
        executor.submit(_lowercase,pipeline)
        executor.submit(_tokenize,pipeline)
        executor.submit(_stopword,pipeline)
        executor.submit(_stem,pipeline)
        executor.submit(_result,pipeline)
        executor.submit(_insert,pipeline)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    # logging.getLogger().setLevel(logging.DEBUG)
    # multiThread()
    
    # pool = mp.Pool()
    # workers = mp.cpu_count() - 1
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

    