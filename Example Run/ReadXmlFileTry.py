from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession

from rake_nltk import RakeKeywordExtractor

class FileNameKeyWordCount:
    def __init__(self, filename, keyWordCount):
        self.__filename = filename
        self.__keyWordCount = keyWordCount
    
    def __str__(self):
        return "{}, {}".format(self.__filename, self.__keyWordCount)

#def saveKeyWords(filename, keywords):
    


def main():
    spark = SparkSession\
        .builder\
        .appName("ReadXmlFileFromBuzzFeed")\
        .getOrCreate()
    
    mainText = spark.read.format("com.databricks.spark.xml")\
        .options(rowTag="article")\
        .load("hdfs://santa-fe:47001/Source-Recommendation-System/BuzzFeed-Webis/articles/0001.xml")\
        .select("mainText").head().mainText

    
    rake = RakeKeywordExtractor()
    keywords = rake.extract(mainText, incl_scores=True)
    
 #   saveKeyWords(keywords)
    print(keywords)
    
    spark.stop()



if __name__ == "__main__":
    main()