import concurrent.futures
import logging
import Queue as queue
import random
import threading
import time
import os
from nltk.tokenize import RegexpTokenizer
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords



def lowercase(queue,queue2,event):    
    while not event.is_set() or not queue.empty():
        message = queue.get()
        data = {
            "id" : message["id"],
            "content": message["content"].lower()
        }
        queue2.put(data)
        # logging.info(
        #     "lowercasing storing message: (size=%d)", queue2.qsize()
        # )
def tokenize(queue,queue2, event):
    while not event.is_set() or not queue.empty():
        message = queue.get()
        tokenizer = RegexpTokenizer(r'(?u)\b[\w\-\'\.\/\\\:]+\w\b')
        token = tokenizer.tokenize(message["content"])
        data = {
            "id": message["id"],
            "content": token
        }
        queue2.put(data)
        # logging.info(
        #     "Tokenizing storing message: (size=%d)",  queue2.qsize()
        # )
def sw_removal(queue,queue2,stop_words, event):
    while not event.is_set() or not queue.empty():
        message = queue.get()
        sw = [w for w in message["content"] if not w in stop_words]
        data = {
            "id": message["id"],
            "content": sw
        }
        queue2.put(data)
        logging.info(
            "SW storing message: (size=%d)", queue2.qsize()
        )
def stem(queue,queue2,stemmer, event):
    while not event.is_set() or not queue.empty():
        message = queue.get()
        words_bag = [stemmer.stem(w) for w in message["content"]]
        data ={
            "id": message["id"],
            "content": words_bag
        }
        queue2.put(data)
        # logging.info(
        #     "Stem storing message:(size=%d)", queue2.qsize()
        # )
def wordCount(queue,queue2, event):
    while not event.is_set() or not queue.empty():
        
        message = queue.get()
        list_tf = {}
        for w in message["content"]:
            if w not in list_tf:
                list_tf[w] = 0
                
            list_tf[w] += 1

        data = {
            "id": message["id"],
            "content": list_tf
        }
        queue2.put(data)
        logging.info(
            "wordCount storing message: (size=%d)",  queue2.qsize()
        )

def producer(queue, event):
    """Pretend we're getting a number from the network."""
    i = 0
    # while not event.is_set():
    while not i > 5:
        message = "my name is dimas and i am a lady killer, someone might kill me in this kind of situation cause i'm too handsome. some cases people keep talking about my face and my godly attitude which can causes the end of the day. no matter what happen it is really difficult to be in side of the world. so i hope people would just keep silence and do nothing. i know i'm handsome enough to causes chaos around the world. because everyone would do everything to just touch me or even have me around them. please be gentleman and do nothing thank you."
        # logging.info("Producer got message: %s", message)
        for index in range(5):
            data = {
                "id":1,
                "content":message
            }
            queue.put(data)
        logging.info("produce data %d", queue.qsize())
        i+=1

    logging.info("Producer received event. Exiting")

def consumer(queue, event):
    """Pretend we're saving a number in the database."""
    while not event.is_set() or not queue.empty():
        message = queue.get()
        logging.info(
            "Consumer storing message: %s (size=%d)", message["content"], queue.qsize()
        )

    logging.info("Consumer received event. Exiting")

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    stemmer = PorterStemmer()
    stop_words = set(stopwords.words("english"))
    pipeline = queue.Queue(maxsize=100)
    lowerQueue = queue.Queue(maxsize=100)
    tokenQueue = queue.Queue(maxsize=100)
    swQueue = queue.Queue(maxsize=100)
    stemQueue = queue.Queue(maxsize=100)
    wordCountQueue = queue.Queue(maxsize=100)
    event = threading.Event()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.submit(producer, lowerQueue, event)
        executor.submit(lowercase, lowerQueue, tokenQueue, event)
        executor.submit(tokenize, tokenQueue, swQueue, event)
        executor.submit(sw_removal, swQueue, wordCountQueue,stop_words, event)
        # executor.submit(stem, stemQueue, wordCountQueue,stemmer, event)
        executor.submit(wordCount, wordCountQueue, pipeline, event)
        executor.submit(consumer, pipeline, event)

        # time.sleep(0.1)
        logging.info("Main: about to set event")
        # event.set()