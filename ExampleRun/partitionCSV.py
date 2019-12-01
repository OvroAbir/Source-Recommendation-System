from pyspark.sql.functions import input_file_name
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

def delete_path(spark, path):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)


total_data_size = 29*1000
each_file_size = 500
numoffiles = total_data_size / each_file_size
#numoffiles = 5
weights = [1.0] * numoffiles

#inputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/FakeNewsCorpus/news_cleaned_2018_02_13.csv"
inputfolderpath = "hdfs://santa-fe:47001/FakeNewsCorpus/news_cleaned_2018_02_13.csv"
#inputfolderpath = "hdfs://santa-fe:47001/Source-Recommendation-System/FakeNewsCorpus/news_sample.csv"
outputfolderpath = "hdfs://santa-fe:47001/FakeNewsCorpus-Outputs/news_cleaned_partitioned"


spark = SparkSession.builder.appName("SplitCSVFileFromFakeNews").getOrCreate()
delete_path(spark, outputfolderpath)
sqlContext = SQLContext(spark.sparkContext)
inputfile = sqlContext.read.csv(inputfolderpath, header=True,sep=",", multiLine = True, quote='"', escape='"')

partitions = inputfile.randomSplit(weights)

count = 0
for df in partitions:
    filename = inputfolderpath.split("/")[-1].split(".")[0] + str(count).zfill(5)
    df.write.csv(outputfolderpath + "/" + filename, header=True, quote='"', escape='"')
    print("saved " + filename + " of " + str(len(partitions)))
    count += 1

spark.stop()

