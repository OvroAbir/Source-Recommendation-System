from pyspark.sql.functions import input_file_name
from rake_nltk import RakeKeywordExtractor
from pyspark.sql.types import *
import sys
import gc

from pyspark.sql import SQLContext

inputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/FakeNewsCorpus/news_sample.csv"
outputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/FakeNewsCorpus-Outputs"
intermediate_filename = "hdfs://santa-fe:47001/Source-Recommendation-System/FakeNewsCorpus-Outputs/intermediate_file.csv"

sqlContext = SQLContext(spark.sparkContext)
inputfile = sqlContext.read.csv(inputfolderpath)
