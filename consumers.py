import pika 
from ast import literal_eval
from nltk.tokenize import RegexpTokenizer
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.corpus import stopwords
from collections import Counter
connection  = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
queue_name = "node2-queue"
channel.queue_declare(queue=queue_name, durable=True)

def callback(ch,method, properties, body):
    print("received %r" % body)
    #use process here.
    content = literal_eval(body)
    wordsList = TermFrequency(content["content"])
    data = {
        "words" : wordsList,
        "doc_id" : content["id"]
    }
    ch.basic_ack(delivery_tag=method.delivery_tag)
def Preprocess(teks):
    # lowercase
    teks = teks.lower()
    
    # Token + remove punctuation 
    #--tokenizer = RegexpTokenizer(r'\w+')
    tokenizer = RegexpTokenizer(r'(?u)\b[\w\-\'\.\/\\\:]+\w\b')
    teks = tokenizer.tokenize(teks)

    # Stopwords
    stop_words = set(stopwords.words('english')) 
    word_tokens = teks

    # teks : array dengan kata tanpa stopword 
    #mapping
    teks = [w for w in word_tokens if not w in stop_words]
    teks = [] 
    for w in word_tokens: 
        if w not in stop_words: 
            teks.append(w) 

    #-- Stemming pake snowball stemmer
    #--ps = SnowballStemmer(language='english')
    #--teks = [ps.stem(kata) for kata in teks] 

    return teks
def TermFrequency(isi):
    teksData = Preprocess(isi)
    list_tfidf = {}
    print (teksData)
    # list_tfidf.append([x, count])
    #reducing
    for word in teksData:
        if word not in list_tfidf:
            list_tfidf[word] = 0
        list_tfidf[word] += 1
    print(list_tfidf)   
    return list_tfidf       
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

print('waiting for message')
channel.start_consuming()