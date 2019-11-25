# from __future__ import division

# import re
# import sys
# from operator import add, concat
# import subprocess

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import input_file_name
# from pyspark.sql.types import *
# from rake_nltk import RakeKeywordExtractor


# # class FileNameKeyWordCount:
# #     def __init__(self, filename, keyWordCount):
# #         self.__filename = filename
# #         self.__keyWordCount = keyWordCount
    
# #     def __str__(self):
# #         return "{}, {}".format(self.__filename, self.__keyWordCount)

# # def saveKeyWords(spark, filename, keyword_counts, outputfolderpath):
    
# #     keyword_filecount_pairs = []

# #     for keyword_count in keyword_counts:
# #         (key, count) = keyword_count
# #         fileNameKeyWordCountStr = str(FileNameKeyWordCount(filename, count))
# #         keyword_filecount_pairs.append((key, fileNameKeyWordCountStr))
    
# #     schema = StructType([
# #         StructField("keyword", StringType(), False),
# #         StructField("filecount", StringType(), False)
# #     ])

# #     keyword_filecount_rdd = spark.createDataFrame(keyword_filecount_pairs, schema=schema).rdd    
# #     keyword_filecount_mapper = keyword_filecount_rdd.map(lambda keyword_filecount: (keyword_filecount[0], keyword_filecount[1]))  
# #     keyword_filecount_reducer_rdd = keyword_filecount_mapper.groupByKey()

# #     keyword_filecount_reducer_df = spark.createDataFrame(keyword_filecount_reducer_rdd, schema=schema)
# #     keyword_filecount_reducer_df.write.csv(outputfolderpath + "/temp.csv")

# #     keyword_filecount_reducer_df.collect()



# # def get_rdd_keyword_filenamescore(spark, filepath, mainText):
# #     rake = RakeKeywordExtractor()
# #     keyword_scores = rake.extract(mainText, incl_scores=True)
# #     keyword_filecount_pairs = []

# #     for keyword_score in keyword_scores:
# #         (key, score) = keyword_score
# #         keyword_filecount_pairs.append((key, (filepath, score)))
    
# #     return spark.sparkContext.parallelize(keyword_filecount_pairs)

# #     #import rake_nltk
# #     rake = RakeKeywordExtractor()
# #     keyword_scores = rake.extract(mainText, incl_scores=True)
    
# #     keyword_filecount_pairs = []

# #     for keyword_score in keyword_scores:
# #         (key, score) = keyword_score
# #         #fileNameKeyWordCountStr = str(FileNameKeyWordCount(filename, count))
# #         keyword_filecount_pairs.append((key, (filepath, score)))

# #     print("---------" + "okaoka")

# #     print(keyword_filecount_pairs)
# #     return keyword_filecount_pairs

#     # schema = StructType([
#     #     StructField("keyword", StringType(), False),
#     #     StructField("filecount", StringType(), False)
#     # ])

# def delete_path(spark, path):
#     sc = spark.sparkContext
#     fs = (sc._jvm.org
#           .apache.hadoop
#           .fs.FileSystem
#           .get(sc._jsc.hadoopConfiguration())
#           )
#     fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)

# def get_rdd_keyword_filenamescore(filepath, mainText):
#     mainText = "fjhr fjkfn krjfn rfjb kfjrn fkrjfrj krjnfk f krjfnfj jrfnr hrfbrhb ehbd h"
#     spl = mainText.split(" ")
#     keyword_scores = [(spl[0], 1.2),(spl[1], 2.2),(spl[2], 5.2),(spl[3], 6.2)]
#     keyword_filecount_pairs = []
#     for keyword_score in keyword_scores:
#         (key, score) = keyword_score
#         keyword_filecount_pairs.append(["qqq", ("ddjj", 1.2)])
#     return keyword_filecount_pairs

# def main():
#     inputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/BuzzFeed-Webis/articles"
#     outputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/TempOutputFolder"
    
#     spark = SparkSession.builder.appName("ReadXmlFileFromBuzzFeed").getOrCreate()

#     inputfile = spark.read.format("com.databricks.spark.xml").options(rowTag="article").load(inputfolderpath)
#     inputfile = inputfile.withColumn("filename", input_file_name())
#     mainText_inputfile_DF = inputfile.select("filename", "mainText")
    
#     keyword_file_scores = mainText_inputfile_DF.rdd\
#         .map(lambda row : get_rdd_keyword_filenamescore(row[0], row[1]))\
#         .flatMap(lambda row : (row[0], row[1]))\
#         .groupByKey()\
#         .map(lambda key_files : (key_files[0], list(key_files[1])))

    


#     # mainText = inputfile.select("mainText").head().mainText

#     # rake = RakeKeywordExtractor()
#     # keywords = rake.extract(mainText, incl_scores=True)
    
#     # delete_path(spark, outputfolderpath)

#     # saveKeyWords(spark, inputfile.select("filename").head().filename, keywords, outputfolderpath)
    
#     spark.stop()


# if __name__ == "__main__":
#     main()

from pyspark.sql.functions import input_file_name
from rake_nltk import RakeKeywordExtractor
from pyspark.sql.types import *
import sys
import gc

def get_rdd_keyword_filenamescore(filepath, mainText):
    if(mainText == None):
        mainText = "fhbf djn djfj ffh kdn kfnh jfnf hbf jfn jnd jfbf"
    rake = RakeKeywordExtractor()
    keyword_scores = rake.extract(mainText, incl_scores=True)
    keyword_filecount_pairs = []
    filepath = filepath.split("/")[-1]
    for keyword_score in keyword_scores:
        (key, score) = keyword_score
        keyword_filecount_pairs.append([key, filepath + ",," + str(score)])
    return keyword_filecount_pairs

def delete_path(spark, path):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)

def save_keywords_list_in_file(inputfolderpath, outputfolderpath, intermediate_filename, spark):
    delete_path(spark, intermediate_filename)
    inputfile = spark.read.format("com.databricks.spark.xml").options(rowTag="article").load(inputfolderpath)
    inputfile = inputfile.withColumn("filename", input_file_name())
    mainText_inputfile_DF = inputfile.select("filename", "mainText")
    keyword_file_score_list = map(lambda row : get_rdd_keyword_filenamescore(row[0], row[1]) , mainText_inputfile_DF.collect())
    spark.sparkContext.parallelize(keyword_file_score_list).saveAsPickleFile(intermediate_filename)

def read_and_reduce_keywords(intermediate_filename, outputfolderpath, schema, spark):
#     delete_path(spark, outputfolderpath)
    keyword_file_scores_rdd = spark.sparkContext.pickleFile(intermediate_filename)
    keyword_file_scores_rdd = keyword_file_scores_rdd.filter(lambda row: len(row) >= 2).flatMap(lambda row : (row[0], row[1]))\
            .filter(lambda row: len(row) >= 2).map(lambda row : (row[0], row[1]))\
            .groupByKey()\
            .map(lambda key_files : (key_files[0], list(key_files[1])))
    keyword_file_scores_df = spark.createDataFrame(keyword_file_scores_rdd, schema=schema)
    #keyword_file_scores_df.write.csv(outputfolderpath)
    return keyword_file_scores_df


inputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/BuzzFeed-Webis/articles"
outputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/KeyWordFileScoreFolder"
intermediate_filename = "hdfs://santa-fe:47001/Source-Recommendation-System/TempOutputFolder/intermediate_file.csv"
schema = StructType([
        StructField("keyword", StringType(), False),
        StructField("file_name_score", StringType(), False)
    ])
save_keywords_list_in_file(inputfolderpath, outputfolderpath, intermediate_filename, spark)
gc.collect()
final_keyword_file_score_df = read_and_reduce_keywords(intermediate_filename, outputfolderpath, schema, spark)
final_keyword_file_score_df.write.csv(outputfolderpath)
print(final_keyword_file_score_df.show(20, False))
