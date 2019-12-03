from pyspark.sql.functions import input_file_name
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import nltk
import sys
from pyspark.sql import SQLContext
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize
from ast import literal_eval
from operator import concat
from rake_nltk import Rake
from pyspark.context import SparkContext

def delete_path(sc, path):
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)

def get_processed_words(title):
    title = str(title.encode("ascii", "ignore"))
    if("/s/chopin/a/grad/joyghosh/nltk_data" not in nltk.data.path):
        nltk.data.path.append("/s/chopin/a/grad/joyghosh/nltk_data")
    stop_words = set(stopwords.words('english')) 
    return [w.lower() for w in word_tokenize(title) if (not w in stop_words and len(w) > 1)]

def extract_with_row_id(id, content):
    try:
        if("/s/chopin/a/grad/joyghosh/nltk_data" not in nltk.data.path):
            nltk.data.path.append("/s/chopin/a/grad/joyghosh/nltk_data")
        text = str(content.encode('ascii', "ignore"))
        global rake
        rake.extract_keywords_from_text(text)
        first_half_keywords = rake.get_ranked_phrases_with_scores()
        first_half_keywords = first_half_keywords[0:len(first_half_keywords)/2]
        return [ (sk[1].lower(), "(" + str(id) + "," + str(round(sk[0], 1))+")") for sk in first_half_keywords ]
    except UnicodeDecodeError:
        return []
    except UnicodeEncodeError:
        return []
    return []

def parse_meta_keywords(txt):
    try:
        txt.strip()
        return literal_eval(txt)
    except:
        print("++Got exception for " + txt)
        return []

def get_keywords_from_keywords_col(txt):
    try:
        return str(row["keywords"].encode('ascii', "ignore")).split(" ")
    except:
        print("++Got exception for keywords col")
        return []

def main(inputfolderpath, outputfolderpath, jobname):
    #inputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/FakeNewsCorpus/news_cleaned_2018_02_13.csv"
    #inputfolderpath = "hdfs://santa-fe:47001/FakeNewsCorpus/news_cleaned_2018_02_13.csv"
    #inputfolderpath = "hdfs://santa-fe:47001/FakeNewsCorpus-Outputs/news_cleaned_partitioned/news_cleaned_2018_02_1300000"
    #inputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/FakeNewsCorpus/news_sample.csv"
    #outputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/FakeNewsCorpus-Outputs"
    #outputfolderpath = "hdfs://santa-fe:47001/FakeNewsCorpus-Outputs/KeywordsFromPartitions/news_cleaned_partitioned/news_cleaned_2018_02_1300000temp"
    title_score = 10
    keywords_score = 13
    meta_keywords_score = 13
    meta_description_score = 13
    tags_score = 13
    summary_score = 10
    #spark = SparkSession.builder.appName(jobname).getOrCreate()
    sc = SparkContext(master="spark://santa-fe.cs.colostate.edu:47002", appName=jobname)
    delete_path(sc, outputfolderpath)
    sqlContext = SQLContext(sc)
    inputfile_rdd = sqlContext.read.csv(inputfolderpath, header=True,sep=",", multiLine = True, quote='"', escape='"')\
        .rdd.repartition(29)
    keywords_from_content = inputfile_rdd\
        .filter(lambda row : row["content"] is not None and row["content"] != "null")\
        .map(lambda  row : extract_with_row_id(row["id"], row["content"]))\
        .flatMap(lambda xs: [(x) for x in xs])
    keywords_from_title = inputfile_rdd\
        .filter(lambda row : row["title"] is not None and row["title"] != "null")\
        .map(lambda row : [(x,"(" + str(row["id"]) + "," + str(title_score) + ")") for x in get_processed_words(row["title"])])\
        .flatMap(lambda xs: [(x) for x in xs])
    keywords_from_keywords_col = inputfile_rdd\
        .filter(lambda row : row["keywords"] is not None and row["keywords"] != "null")\
        .map(lambda row : [(x.lower(),"(" + str(row["id"]) + "," + str(keywords_score) + ")") for x in get_keywords_from_keywords_col(row["keywords"])])\
        .flatMap(lambda xs: [(x) for x in xs])
    keywords_from_meta_keywords = inputfile_rdd\
        .filter(lambda row : row["meta_keywords"] is not None and row["meta_keywords"] != "null")\
        .map(lambda row : [(x.lower(),"(" + str(row["id"]) + "," + str(meta_keywords_score) + ")") for x in parse_meta_keywords(row["meta_keywords"]) if len(x) > 1 ])\
        .flatMap(lambda xs: [(x) for x in xs])
    keywords_from_meta_description = inputfile_rdd\
        .filter(lambda row : row["meta_description"] is not None and row["meta_description"] != "null")\
        .map(lambda row : [(x, "(" + str(row["id"]) + "," + str(meta_description_score) + ")") for x in get_processed_words(row["meta_description"])])\
        .flatMap(lambda xs: [(x) for x in xs])
    keywords_from_tags = inputfile_rdd\
        .filter(lambda row : row["tags"] is not None and row["tags"] != "null")\
        .map(lambda row : [(x.lower(), "(" + str(row["id"]) + "," + str(tags_score) + ")") for x in str(row["tags"].encode('ascii', "ignore")).split(",") ])\
        .flatMap(lambda xs: [(x) for x in xs])
    keywords_from_summary = inputfile_rdd\
        .filter(lambda row : row["summary"] is not None and row["summary"] != "null")\
        .map(lambda  row : extract_with_row_id(row["id"], row["summary"]))\
        .flatMap(lambda xs: [(x) for x in xs])
    all_keywords_list = [keywords_from_content, keywords_from_title, keywords_from_keywords_col, keywords_from_meta_keywords,
        keywords_from_meta_description, keywords_from_tags, keywords_from_summary]
    all_keywords_rdd = sc.union(all_keywords_list)
    all_keywords_rdd = all_keywords_rdd\
        .filter(lambda row: len(row[0]) > 2)\
        .reduceByKey(concat)
    all_keywords_df = all_keywords_rdd.toDF(["Keyword", "RowId & Score"])
    all_keywords_df.write.csv(outputfolderpath, header=True, quote='"', escape='"')
    sc.stop()

rake = Rake()
inputfolderpath = sys.argv[1]
outputfolderpath = sys.argv[2]
jobname = sys.argv[3]

main(inputfolderpath, outputfolderpath, jobname)

