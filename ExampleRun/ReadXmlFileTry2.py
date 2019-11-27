from pyspark.sql.functions import input_file_name
from pyspark.sql import SparkSession
from rake_nltk import RakeKeywordExtractor
from pyspark.sql.types import *

def delete_path(spark, path):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)


spark = SparkSession.builder.appName("ReadXmlFileFromBuzzFeed").getOrCreate()
inputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/BuzzFeed-Webis/articles"
outputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/KeyWordFileScoreFolder"
intermediate_filename = "hdfs://santa-fe:47001/Source-Recommendation-System/TempOutputFolder/intermediate_file.csv"
schema = StructType([   
        StructField("keyword", StringType(), False),
        StructField("file_name&score", StringType(), False)
    ])

delete_path(spark, intermediate_filename)
delete_path(spark, outputfolderpath)
inputfile = spark.read.format("com.databricks.spark.xml").options(rowTag="article").load(inputfolderpath)
inputfile = inputfile.withColumn("filename", input_file_name())
mainText_inputfile_DF = inputfile.select("filename", "mainText")
rake = RakeKeywordExtractor()
keyword_file_scores_rdd = mainText_inputfile_DF.rdd\
        .map(lambda row : rake.extract_with_filename(row["mainText"], row["filename"], True))\
        .flatMap(lambda xs: [(x) for x in xs])\
        .filter(lambda row: len(row) >= 2)\
        .groupByKey()\
        .map(lambda key_files : (key_files[0], list(key_files[1])))
keyword_file_scores_df = spark.createDataFrame(keyword_file_scores_rdd, schema=schema)
keyword_file_scores_df.write.csv(outputfolderpath)
keyword_file_scores_df.show()
