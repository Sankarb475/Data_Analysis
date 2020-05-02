# Data_Analysis
Ingesting data using Kafka to Spark streaming for processing and uploading the conformed data to Cassandra

Kafka Producer => Reading a csv file and streaming it to a topic

Spark Streaming => Being the kafka consumer of that topic, is receiving the csv data, manipulates the data

Cassandra => Manipulated, conformed data from Spark streaming is getting ingested to Cassandra installed in local 
