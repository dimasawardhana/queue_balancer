import pika
from ast import literal_eval
import os
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer, PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.corpus import stopwords
from collections import Counter
import multiprocessing
stemmer = PorterStemmer()
def mapping(teks) :
    print "mappping initialized"
    #lowercasing
    lowercase =  teks.lower()
    print "lowercase finished"
    #tokenizing - tokenize and remove punctuation
    tokenizer = RegexpTokenizer(r'(?u)\b[\w\-\'\.\/\\\:]+\w\b')
    token = tokenizer.tokenize(lowercase)
    print "tokenize finished"
    #remove stopword
    stop_words = set(stopwords.words("english"))
    word_tokens = token
    print "stopword removal finish"
    #stemming
    
    # words_bag = [stemmer.stem(w) for w in word_tokens]
    # for w in word_tokens:
    #     words_bag = words_bag.append(lemmatizer.lemmatize(w))
    # map_result = [w for w in words_bag if not w in stop_words]
    map_result = [w for w in word_tokens if not w in stop_words]
    print "mapping finished"
    return map_result

def reduce(map_result):
    print "reduce initialized"
    list_tf = {}
    for w in map_result:
        if w not in list_tf:
            list_tf[w] = 0
        list_tf[w] += 1
    return list_tf

def callback(ch, method, properties, body):
    dataToProcess = literal_eval(body)
    data = dataToProcess["content"]
    process = reduce(mapping(data))
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume():
    print "ready pool # ", multiprocessing.current_process().name, "\n"
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))

    channel = connection.channel()
    channel.queue_declare(queue="queue_1",durable=True)
    
    channel.basic_qos(prefetch_count=20)
    channel.basic_consume(queue="queue_1", on_message_callback=callback, auto_ack=False)
    
    print ' [*] Waiting for messages. To exit press CTRL+C', "\n"
    try :
        channel.start_consuming()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    pool = multiprocessing.Pool()
    # workers = multiprocessing.cpu_count() 
    workers = 3
    for i in xrange(0, workers):
        print "hai ",i
        pool.apply_async(consume)

    try:
        while True:
            continue
    except KeyboardInterrupt:
        print ' [*] Exiting...'
        pool.terminate()
        pool.join()


