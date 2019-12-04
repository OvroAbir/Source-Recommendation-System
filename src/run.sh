#spark-submit --packages com.databricks:spark-csv_2.10:0.4.1 --py-files rake_nltk.py ReadCSVFileTry.py

#$SPARK_HOME/bin/spark-submit --master yarn --num-executors 30 --deploy-mode cluster --packages com.databricks:spark-csv_2.10:1.5.0 newtry.py


#$SPARK_HOME/bin/spark-submit --supervise --driver-memory 2G --executor-memory 2G --num-executors 56 ReadCSVFileTry5.py hdfs://richmond:53001/FakeNewsCorpus/news_cleaned_2018_02_13.csv hdfs://richmond:53001/news_cleaned_2018_02_1300016 job-016

#$SPARK_HOME/bin/spark-submit --supervise --driver-memory 2G --executor-memory 2G --num-executors 56 --deploy-mode cluster --packages com.databricks:spark-csv_2.10:1.5.0 newtry.py

$SPARK_HOME/bin/spark-submit --driver-memory 2G --executor-memory 2G --num-executors 60 --packages com.databricks:spark-xml_2.10:0.4.1 ExtractKeywordsFromBuzzfeed.py
