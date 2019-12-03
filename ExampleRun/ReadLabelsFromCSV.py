from pyspark.sql import SQLContext
from pyspark.context import SparkContext

app_name = "GetLabelsFromCSV"
#spark = SparkSession.builder.appName(app_name).getOrCreate()
sc = SparkContext(master="local[1]", appName=app_name)
sqlContext = SQLContext(sc)
inputfolder = "hdfs://santa-fe:47001/FakeNewsCorpus-Outputs/news_cleaned_partitioned/news_cleaned_2018_02_1300000"
inputfile_rdd = sqlContext.read.csv(inputfolder, header=True,sep=",", multiLine = True, quote='"', escape='"')\
    .rdd.repartition(29)

id_list = ["1119642","1404141", "249706"]

selected_rows_from_input = inputfile_rdd\
    .filter(lambda row: row["id"] in id_list)\
    .map(lambda row: (row["id"], row["type"]))

selected_rows_from_input.collect()
