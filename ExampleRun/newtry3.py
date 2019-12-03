from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from rake_nltk import Rake
from fuzzywuzzy import fuzz
from operator import concat
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 
import traceback, socket

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

def match_phrases(keyword, id_score):
    # print("++machine is " + socket.gethostbyname(socket.gethostname()))
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
            if(not text.__contains__(keyword)):
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

spark = SparkSession \
    .builder \
    .appName("Reading from Fake News Corpus") \
    .getOrCreate()
sc = spark.sparkContext

sqlContext = SQLContext(sc)
inputfolderpath2 = "hdfs://richmond:53001/SampleInputs/keyword_input.csv"

schema2 = StructType([ \
    StructField("Keyword", StringType(), True), \
    StructField("RowId & Score", StringType(), True)])
inputfileDF = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true', inferschema='true', sep=",", multiLine = True, quote='"', escape='"') \
    .load(inputfolderpath2, schema = schema2)
inputfileDF.repartition(3)

# inputfileDF = inputfileDF.rdd\
#     .reduceByKey(concat)
#     # .filter(lambda row: row[1] is None or row[1]=='null')

# newDF = spark.createDataFrame(inputfileDF)

# print(newDF.dtypes)
# newDF.show()
# print(newDF.count())
textinputfile="/s/chopin/k/grad/deotales/Source-Recommendation-System/ExampleRun/input.txt"
file1 = open(textinputfile,"r")
text = file1.read()
text = str(text.encode('ascii', "ignore"))
file1.close() 
rake = Rake()
rake.extract_keywords_from_text(text)
keyphrases_w_scores = rake.get_ranked_phrases_with_scores()
keyphrases = rake.get_ranked_phrases()

inputfileDF = inputfileDF.rdd\
    .flatMap(lambda row: match_phrases(row[0], row[1]))\
    .repartition(3)

inputfileDF = spark.createDataFrame(inputfileDF)
inputfileDF.repartition(3)
inputfileDF.show()
print(inputfileDF.count())
spark.stop()