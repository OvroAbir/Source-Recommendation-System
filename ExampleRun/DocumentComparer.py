import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
import gensim


class DocumentCompare:
    def __init__(self, doc1, doc2, workdir):
        self.doc1 = doc1
        self.doc2 = doc2 # list of query docs
        self.workdir = workdir

    def compare(self):
        tokens = sent_tokenize(self.doc1)
        file_docs = []
        file2_docs = []
        for line in tokens:
            file_docs.append(line)
            
        gen_docs = [[w.lower() for w in word_tokenize(self.doc1)]]
        dictionary = gensim.corpora.Dictionary(gen_docs)
        corpus = [dictionary.doc2bow(gen_doc) for gen_doc in gen_docs]
        tf_idf = gensim.models.TfidfModel(corpus)
        sims = gensim.similarities.Similarity('workdir/',tf_idf[corpus],
                                        num_features=len(dictionary))


        tokens = sent_tokenize(self.doc2)
        for line in tokens:
            file2_docs.append(line)

        query_doc = [[w.lower() for w in word_tokenize(self.doc2)]]
        # for line in file2_docs:
        #     query_doc = [w.lower() for w in word_tokenize(line)]
            
        query_doc_bow = [dictionary.doc2bow(q_doc) for q_doc in query_doc] 

        query_doc_tf_idf = tf_idf[query_doc_bow]

        cosine = sims[query_doc_tf_idf][0][0]
        print('Sims: ' + str(cosine))
        similarity = round((1-cosine)*100,2)
        print('Similarity: ' + str(similarity) + '%')



# textinputfile="/s/chopin/k/grad/deotales/Source-Recommendation-System/ExampleRun/input.txt"
textinputfile="input1.txt"
file1 = open(textinputfile,"r")
text = file1.read()
# text = str(text.encode('ascii', "ignore"))
file1.close()
textinputfile="input2.txt"
file1 = open(textinputfile,"r")
text2 = file1.read()
# text = str(text.encode('ascii', "ignore"))
file1.close()
dc = DocumentCompare(text, text2, 'workdir')
dc.compare()