# Source-Recommendation-System
Source Recommendation System takes an article from the user as input and outputs any relevant article from 8.5 million articles in the dataset to the user. It uses Apache Spark to handle this huge load of articles.

## Prerequisites
This project uses [rake-nltk](https://pypi.org/project/rake-nltk/) library to extract keywords.
```
pip install rake-nltk
```

[FakeNewsCorpus](https://github.com/several27/FakeNewsCorpus) was used as dataset (27 GB) for news articles. [Apache Spark](https://spark.apache.org/) has been used to handle this huge dataset. It needs to be correctly installed and configured. The configuration file for Spark can be found at spark-2.4.4-bin-hadoop2.7 folder. [Hadoop](https://hadoop.apache.org/) was used as underlying distributed file system. The configuration for Hadoop can be found at hadoop-conf folder. Both of them needs to changed according to your configuration.


## Source Code
The source code can be found at /src folder. 
* The whole dataset was partitioned into smaller files. The code to partition dataset can be found at [PartitionFakeNewsCorpus.py](src/PartitionFakeNewsCorpus.py) file.
* The code to extract keywords from partitioned dataset can be found at [ExtractKeywordsFromFakeCorpus.py](src/ExtractKeywordsFromFakeCorpus.py) file.
* The main code to input article and to output relevant articles can be found at [FindSimillarDocs.py](src/FindSimillarDocs.py) file.

## Algorithm & Implementation Details
This idea was implement as project for course work of [Distributed System](https://www.cs.colostate.edu/~cs555/) course in Colorado State Univeristy. 
Detailed description of the algorithm can be found here - 
* [Summary](docs/Source-Recommendation-System_Summary.pdf)
* [Detailed Report](docs/Source-Recommendation-System_Report.pdf)

## Authors
* [Joy Ghosh](https://www.ijoyghosh.com)
* [Saurabh Deotale](https://github.com/sd-26)
