from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from rake_nltk import Rake
from fuzzywuzzy import fuzz
from operator import concat
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize
import nltk, string
import traceback
# import re
from sklearn.feature_extraction.text import TfidfVectorizer

def cosine_similarity_score(X, Y):
    cosine = 0
    try:
        if(X.strip()=='' or Y.strip()==''):
            return 0
        X_list = word_tokenize(X)  
        Y_list = word_tokenize(Y) 
        
        # sw contains the list of stopwords 
        sw = stopwords.words('english')  
        l1 =[];l2 =[] 
        
        # remove stop words from string 
        X_set = {w for w in X_list if not w in sw}  
        Y_set = {w for w in Y_list if not w in sw} 
        
        # form a set containing keywords of both strings  
        rvector = X_set.union(Y_set)  
        for w in rvector: 
            if w in X_set: l1.append(1) # create a vector 
            else: l1.append(0) 
            if w in Y_set: l2.append(1) 
            else: l2.append(0) 
        c = 0
        
        # cosine formula  
        for i in range(len(rvector)): 
                c+= l1[i]*l2[i] 
        cosine = c / float((sum(l1)*sum(l2))**0.5) 
    except:
        cosine=0
    return cosine

def compare_phrase(p1, p2):
    p1 = re.sub('[^A-Za-z0-9 ]+', '', p1).split(' ')
    if(len(set(p1).intersection(set(p2))) > 0):
        return True
    else:
        return False

def match_phrases(keyword, id_score):
    result = []
    global text
    global keyphrases_w_scores
    try:
        keyword = str(keyword.encode('ascii', "ignore"))
        id_score = str(id_score)
        if (id_score==None):
            # print('None type values for '+ keyword)
            return []
        phrase_list = [x[1] for x in keyphrases_w_scores]
        # schema for output -> tuples -> (score, list[id])
        if(phrase_list.__contains__(keyword)):
            # for all id_score value pair get id
            # add pair of score of keyword
            id_score = id_score.replace('(','').split(')')
            while('' in id_score) : 
                id_score.remove('')
            # print(id_score)
            score = keyphrases_w_scores.__getitem__(phrase_list.index(keyword))[0]
            for item in id_score:
                result.append((score,item))
        else:
            p2 = re.sub('[^A-Za-z0-9 ]+', '', text).split(' ')
            if(not compare_phrase(keyword, p2)):
                return []
            # search for each word in phrase
            id_score = id_score.replace('(','').split(')')
            while('' in id_score) : 
                id_score.remove('')
            for phrase in phrase_list:
                ratio = cosine_similarity_score(keyword, phrase)
                if(ratio > 0):
                    # for all id_score value pair get id
                    score = (float)(keyphrases_w_scores.__getitem__(phrase_list.index(phrase))[0])
                    for item in id_score:
                        result.append(((score*ratio),item))
    except:
        # print('None values for '+ keyword)
        traceback.print_exc()
    return result

def save_file(index, id, label, content, folder):
    id = str(id.encode("ascii", "ignore"))
    label = str(label.encode("ascii", "ignore"))
    content = str(content.encode("ascii", "ignore"))
    filename = folder + "/" + str(index) + "_" + str(id.encode() + ".txt")
    file = open(filename, "w")
    file.write(id + "\n" + label + "\n" + content)
    file.close()
    return filename

def map_scored_ids(keyscore, id_score):
    result = []
    keyscore = float(keyscore)
    id_score = id_score.replace('(','').split(')')
    while('' in id_score) : 
        id_score.remove('')
    for item in id_score:
        el = item.split(',')
        id = el[0]
        score =float(el[1])
        final_score = keyscore + score
        result.append((id, final_score))
    return result

spark = SparkSession \
    .builder \
    .appName("Match keywords") \
    .master("spark://richmond.cs.colostate.edu:53101") \
    .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
#hdfs://richmond:53001/SampleInputs/keyword_input.csv
#hdfs://santa-fe:47001//FakeNewsCorpus-Outputs/KeywordsFromPartitions/news_cleaned_partitioned/news_cleaned_2018_02_1300000
inputfolderpath2 = "hdfs://richmond:53001/SampleInputs/keyword_input.csv"

schema2 = StructType([ \
    StructField("Keyword", StringType(), True), \
    StructField("RowId & Score", StringType(), True)])
inputfileRDD = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true', inferschema='true', sep=",", multiLine = True, quote='"', escape='"') \
    .load(inputfolderpath2, schema = schema2).rdd.repartition(30)

textinputfile="/s/chopin/k/grad/deotales/Source-Recommendation-System/ExampleRun/input.txt"
file1 = open(textinputfile,"r")
text = file1.read()
text = str(text.encode('ascii', "ignore"))
file1.close() 
rake = Rake()
rake.extract_keywords_from_text(text)
keyphrases_w_scores = rake.get_ranked_phrases_with_scores()
keyphrases_w_scores = keyphrases_w_scores[0:len(keyphrases_w_scores)/2]
keyphrases = rake.get_ranked_phrases()

inputfileRDD = inputfileRDD\
    .flatMap(lambda row: match_phrases(row[0], row[1]))\
    .flatMap(lambda row: map_scored_ids(row[0], row[1]))\
    .reduceByKey(lambda a, b: (float(a))+(float(b)))\
    .top(15, key=lambda x: x[1])

# print(inputfileRDD.count())
id_list_w_scores = inputfileRDD
id_list = [x[0] for x in id_list_w_scores]
print(id_list)

input_partitioned_folder = "hdfs://santa-fe:47001/FakeNewsCorpus-Outputs/news_cleaned_partitioned/news_cleaned_2018_02_1300000"
whole_inputfile_rdd = sqlContext.read.csv(input_partitioned_folder, header=True,sep=",", multiLine = True, quote='"', escape='"')\
    .rdd.repartition(30)

# Similarity score logic
stemmer = nltk.stem.porter.PorterStemmer()
remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)

def stem_tokens(tokens):
    return [stemmer.stem(item) for item in tokens]

#'''remove punctuation, lowercase, stem'''
def normalize(text):
    return stem_tokens(nltk.word_tokenize(text.lower().translate(remove_punctuation_map)))

vectorizer = TfidfVectorizer(tokenizer=normalize, stop_words='english')

def cosine_sim(text1, text2):
    text2 = str(text2.encode('ascii', "ignore"))
    tfidf = vectorizer.fit_transform([text1, text2])
    return ((tfidf * tfidf.T).A)[0,1]
#

#similarity score for each document needs to be calculated
# simScore = whole_inputfile_rdd\
#     .filter(lambda row: row["id"] in id_list)\
#     .map(lambda row: cosine_sim(text, row["content"], row["id"]))\
#     .take(15)

# print(simScore)

def getsim_score(text_inp):
    return cosine_sim(text, text_inp)


selected_rows_from_input = whole_inputfile_rdd\
    .filter(lambda row: row["id"] in id_list)\
    .map(lambda row: (row["id"], row["type"], row["content"], getsim_score(row["content"])))

selected_rows_from_input_list = selected_rows_from_input.collect()

filecount = 0
output_documents_folder = " /s/chopin/k/grad/deotales/Source-Recommendation-System/ExampleRun"
for id in id_list:
    for row in selected_rows_from_input_list:
        if(id == str(row[0].encode("ascii", "ignore"))):
            print(save_file(filecount, id, row[1], row[2], output_documents_folder))
            filecount += 1

spark.stop()
