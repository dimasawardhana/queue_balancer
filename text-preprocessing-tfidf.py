import pandas as pd
import numpy as np
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
import math
#from nltk.stem import SnowballStemmer
from sklearn.model_selection import StratifiedKFold
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import KNeighborsClassifier
from sklearn import metrics
#from progress.bar import Bar
from collections import Counter

'''
    Return array of word
'''
'''
x = np.array([['a','b'], ['c','d','e'], ['a','b'], ['c','d']])
y = np.array(['x','x','y','y'])
skf = StratifiedKFold(n_splits=2)
skf.get_n_splits(x, y)
for train_index, test_index in skf.split(x,y):
    print('Train:', train_index, 'Test:', test_index)
    x_train, x_test = x[train_index],x[test_index]
    y_train, y_test = y[train_index],y[test_index]
quit()
'''
'''
corpus = [
    'This is the first document.',
    'This document is the second document.',
    'And this is the third one.',
    'Is this the first document?',
]
vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(corpus)
print(vectorizer.get_feature_names())
print(X)
quit()
'''
class NewsClassification:
    def Preprocess(self, teks):
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
        teks = [w for w in word_tokens if not w in stop_words]
        teks = [] 
        for w in word_tokens: 
            if w not in stop_words: 
                teks.append(w) 

        #-- Stemming pake snowball stemmer
        #--ps = SnowballStemmer(language='english')
        #--teks = [ps.stem(kata) for kata in teks] 

        return teks
    
    
    def main(self):
        # ambil data di csv pake pandas
        df = pd.read_csv("dataset/data_100.csv",encoding="latin")
              
        # iterasi data dari csv
        news_arr = []
        type_arr = []
        corpus = [] #tfidf
        for index, row in df.iterrows():
            news = self.Preprocess(row["content"])
            type_news = row["author"]
            corpus.append(row["content"])#tfidf
            #array of array
            news_arr.append(news)
            #--if len(news)<2 :
            #--    print(len(news))
            #--    quit()
            #array of string
            type_arr.append(type_news)
            #--if index == 1:
            #--    print(news_arr)
            #--    print(type_arr)
            #--    break
       
        skf = StratifiedKFold(n_splits=2)
        #--skf.get_n_splits(news_arr, type_arr)
        iterasi = 1
        for train_index, test_index in skf.split(news_arr, type_arr):
            print('Iterasi:', iterasi)
            print('Train:', train_index, 'Test:', test_index)
            corpus_train = [] # corpus_train : array yang menyimpan teks berita untuk data-data train
            corpus_test = [] # corpus_test : array yang menyimpan teks berita untuk data-data test
            # Untuk tfidf data train
            for data_train in train_index :
                corpus_train.append(corpus[data_train]) # data_train : indeks dari data train, train_index : array yang menyimpan indeks data train
            vectorizer_train = TfidfVectorizer(tokenizer=self.Preprocess, smooth_idf=False, sublinear_tf=True, norm=None) # proses hitung tfidf, dengan basis e
            X = vectorizer_train.fit(corpus_train)
            Y = vectorizer_train.get_feature_names()
            Z = vectorizer_train.fit_transform(corpus_train)
            # print(Y) # daftar fitur atau term yang ada pada data train
            #print(corpus_test)
            # print(Z) # tdm : berisi (dokumen, term[ke berapa di array fitur]) nilai idf-nya
            # Untuk tfidf data test
            # print("=====================term=======================")
           
            prob = Z.tocoo().data.tolist()
            doc = Z.tocoo().row.tolist()
            term = Z.tocoo().col.tolist()
            matrix_train = []
            ndoc = len(train_index)
            for d in range(len(set(doc))):
                matrix_train.append([])
            # print(matrix)
            for i in range(len(prob)):
                matrix_train[doc[i]].append([Y[term[i]],prob[i]])
                # matrix_train[doc[i]].append([Y[term[i]], prob[i]])
            print("=====================================oy=====================================")
            print("a",matrix_train[0])
            print("b",matrix_train[1])
            print("c",matrix_train[2])
            # quit()
            # quit(matrix[0])
            matrix_test = []
            for data_test in test_index :
                teks = self.Preprocess(corpus[data_test])
                list_tfidf = []
                nword = len(teks)
                # print(list(zip(Counter(teks).keys(), Counter(teks).values())))
                print("==== tfidf document ====")
                for x, y in list(zip(Counter(teks).keys(), Counter(teks).values())):
                    if(x in Y):
                        count = 0
                        for sublist in matrix_train:
                            for word in sublist:
                                if(x == word[0]):
                                    count += 1
                                    break
                        # tfidf = y * math.log(ndoc/count)
                        # list_tfidf.append([x, tfidf])
                        list_tfidf.append([x, count])
                # print(list_tfidf)
                print("=======================vector============================")
                matrix=[]
                # matrix = matrix_train.copy()
                # matrix.append(list_tfidf)
               
            iterasi += 1

            print("===================================================")
              
news_classification = NewsClassification()
news_classification.main()
print("end")
