#rm ~/Desktop/out.txt
#spark-submit --packages com.databricks:spark-xml_2.10:0.4.1 --py-files rake_nltk.py ReadXmlFileTry.py &> out.txt
spark-submit --packages com.databricks:spark-xml_2.10:0.4.1 --py-files rake_nltk.py ReadXmlFileTry.py
