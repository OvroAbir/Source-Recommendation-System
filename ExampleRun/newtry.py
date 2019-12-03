from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from rake_nltk import Rake
from fuzzywuzzy import fuzz
from operator import concat
# from rake_nltk import RakeKeywordExtractor
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize
import nltk

def match_phrases(inp_phrase_list, keyword, id_score):
    result = []
    try:
        keyword = str(keyword.encode('ascii', "ignore"))
        id_score = str(id_score)
        if (keyword==None or id_score==None):
            print('None values for '+ keyword + ':' +id_score)
            return []
        phrase_list = [x[1] for x in inp_phrase_list]
        # schema for output -> tuples -> (score, list[id])
        if(phrase_list.__contains__(keyword)):
            # for all id_score value pair get id
            # add pair of score of keyword
            id_score = id_score.replace('(','').split(')').remove('')
            score = inp_phrase_list.__getitem__(phrase_list.index(keyword))[0]
            for item in id_score:
                result.append((score + '-inp_file_keyw_score',item))
            
        else:
            # search for each word in phrase
            for phrase in phrase_list:
                ratio = fuzz.ratio(keyword, phrase)
                if(ratio > 0):
                    # for all id_score value pair get id
                    id_score = id_score.replace('(','').split(')').remove('')
                    score = (float)(inp_phrase_list.__getitem__(phrase_list.index(phrase))[0])
                    for item in id_score:
                        result.append((score/2 + '-inp_file_keyw_score',item))
    except:
        print('None values for '+ keyword)
    return result

spark = SparkSession \
    .builder \
    .appName("Reading from Fake News Corpus") \
    .getOrCreate()
sc = spark.sparkContext

# inputfolderpath = "hdfs://richmond:53001/FakeNewsCorpus/news_cleaned_2018_02_13.csv"
inputfolderpath2 = "hdfs://richmond:53001/SampleInputs/keyword_input.csv"
# outputfolderpath = "hdfs://richmond:53001/FakeNewsCorpus-Outputs"
sqlContext = SQLContext(sc)
# schema = StructType([ \
#     StructField(" ", IntegerType(), True), \
#     StructField("id", IntegerType(), True), \
#     StructField("domain", StringType(), True), \
#     StructField("type", StringType(), True), \
#     StructField("url", StringType(), True), \
#     StructField("content", StringType(), True), \
#     StructField("scraped_at", StringType(), True), \
#     StructField("inserted_at", StringType(), True), \
#     StructField("updated_at", StringType(), True), \
#     StructField("title", StringType(), True), \
#     StructField("authors", StringType(), True), \
#     StructField("keywords", StringType(), True), \
#     StructField("meta_keywords", StringType(), True), \
#     StructField("meta_description", StringType(), True), \
#     StructField("tags", StringType(), True), \
#     StructField("summary", StringType(), True)])
schema2 = StructType([ \
    StructField("Keyword", StringType(), True), \
    StructField("RowId & Score", StringType(), True)])
inputfileDF = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true', inferschema='true', sep=",", multiLine = True, quote='"', escape='"') \
    .load(inputfolderpath2, schema = schema2)
# inputfileDF2 = sqlContext.read.format('com.databricks.spark.csv') \
#     .options(header='true', inferschema='true', sep=",", multiLine = True, quote='"', escape='"') \
#     .load(inputfolderpath, schema = schema)

# inputfileDF2 = inputfileDF2\
#     .select("id", "type", "url", "content", "title", "summary")\
#     .filter(inputfileDF2.content is not None and inputfileDF2.content != "null")\
#     .where(inputfileDF2.id == 5529275)

# print(inputfileDF2.take(1))

textinputfile="/s/chopin/k/grad/deotales/Source-Recommendation-System/ExampleRun/input.txt"
file1 = open(textinputfile,"r")
text = file1.read()
file1.close()
rake = Rake()
rake.extract_keywords_from_text(text)
keyphrases_w_scores = rake.get_ranked_phrases_with_scores()
keyphrases_w_scores = keyphrases_w_scores[0:len(keyphrases_w_scores)/2]
keyphrases = rake.get_ranked_phrases()

inputfileDF = inputfileDF.rdd\
    .flatMap(lambda row: match_phrases(keyphrases_w_scores, row[0], row[1]))


reducedDF = spark.createDataFrame(inputfileDF)
reducedDF.show()

spark.stop()
