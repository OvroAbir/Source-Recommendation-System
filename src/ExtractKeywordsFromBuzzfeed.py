from pyspark.sql.functions import input_file_name
from pyspark.sql import SparkSession
#from rake_nltk import RakeKeywordExtractor
from rake_nltk import Rake
from pyspark.sql.types import *

def delete_path(spark, path):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)

def extract_with_filename(mainText, filename):
    ouput = []
    global rake
    mainText = text = str(mainText.encode('ascii', "ignore"))
    rake.extract_keywords_from_text(mainText)
    score_keys = rake.get_ranked_phrases_with_scores()
    for sk in score_keys:
        ouput.append( (str(sk[1]), str(filename).split("/")[-1] + "," + str(round(sk[0], 2)) ))
    return ouput

spark = SparkSession.builder.appName("ReadXmlFileFromBuzzFeed").getOrCreate()
inputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/BuzzFeed-Webis/articles"
outputfolderpath = "hdfs://richmond:53001/BuzzFeed-Webis-Outputs"
schema = StructType([   
        StructField("keyword", StringType(), False),
        StructField("filename&score", StringType(), False)
    ])

delete_path(spark, outputfolderpath)
inputfile = spark.read.format("com.databricks.spark.xml").options(rowTag="article").load(inputfolderpath)
inputfile = inputfile.withColumn("filename", input_file_name())
mainText_inputfile_DF = inputfile.select("filename", "mainText")
rake = Rake()
keyword_file_scores_rdd = mainText_inputfile_DF.rdd\
        .filter(lambda row: row["mainText"] is not None)\
        .map(lambda row : extract_with_filename(row["mainText"], row["filename"]))\
        .flatMap(lambda xs: [(x) for x in xs])\
        .filter(lambda row: len(row) >= 2)\
        .groupByKey()\
        .map(lambda key_files : (key_files[0], list(key_files[1])))
keyword_file_scores_df = spark.createDataFrame(keyword_file_scores_rdd, schema=schema)
keyword_file_scores_df.write.csv(outputfolderpath)
#keyword_file_scores_df.show()
