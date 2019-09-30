import pika
from ast import literal_eval
import os
from nltk.tokenize import RegexpTokenizer
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.corpus import stopwords
from collections import Counter
import multiprocessing
stemmer = PorterStemmer()
# lemmatizer = WordNetLemmatizer()
def mapping(teks) :
    
    #lowercasing
    lowercase =  teks.lower()
    
    #tokenizing - tokenize and remove punctuation
    tokenizer = RegexpTokenizer(r'(?u)\b[\w\-\'\.\/\\\:]+\w\b')
    token = tokenizer.tokenize(lowercase)
    
    #remove stopword
    stop_words = set(stopwords.words("english"))
    word_tokens = token
    
    #stemming
    
    words_bag = [stemmer.stem(w) for w in word_tokens]
    # words_bag = []
    # for w in word_tokens:
    #     words_bag = words_bag.append(lemmatizer.lemmatize(w))
    map_result = [w for w in words_bag if not w in stop_words]
    # map_result = [w for w in word_tokens if not w in stop_words]
    
    return map_result

def reduce(map_result):
    
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
    print "process finished, result : \n"
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume():
    print "ready pool # ", multiprocessing.current_process().name, "\n"
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))

    channel = connection.channel()
    channel.queue_declare(queue="queue_coba",durable=True)
    
    channel.basic_qos(prefetch_count=50)
    channel.basic_consume(queue="queue_coba", on_message_callback=callback, auto_ack=False)
    
    print ' [*] Waiting for messages. To exit press CTRL+C', "\n"
    try :
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
def debugging():
    anjay = "my name is dimas and i am a lady killer, someone might kill me in this kind of situation cause i'm too handsome. some cases people keep talking about my face and my godly attitude which can causes the end of the day. no matter what happen it is really difficult to be in side of the world. so i hope people would just keep silence and do nothing. i know i'm handsome enough to causes chaos around the world. because everyone would do everything to just touch me or even have me around them. please be gentleman and do nothing thank you."
    joy = reduce(mapping(anjay))
    print joy
if __name__ == '__main__':
    pool = multiprocessing.Pool()
    # workers = multiprocessing.cpu_count() - 1
    workers = 1
    for i in xrange(0, workers):
        print "hai ",i
        # pool.apply_async(consume)
        pool.apply_async(debugging)

    try:
        while True:
            continue
    except KeyboardInterrupt:
        print ' [*] Exiting...'
        pool.terminate()
        pool.join()


