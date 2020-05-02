package com.apple.utilities

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSpark extends App{
  System.setProperty("hadoop.home.dir", "D:\\App\\hadoop_2.7.1")

  val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val sc = spark.sparkContext

  val ssc = new StreamingContext(sc, Seconds(10))

  val topicsSet = Set("test-log")


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafka_spark",
    "auto.offset.reset" -> "latest"
  )

  //consume the messages from Kafka topic and create DStream
  val LogsStream = KafkaUtils
    .createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String]
      (topicsSet,kafkaParams))

  val messages=LogsStream.map(_.value())

  case class schema(id: Int, first: String, last: String, monthly_salary: Int, currency: String)

  import spark.implicits._

  messages.foreachRDD(
    rdd => {
      val df = rdd.map(x => x.split(',')).map(field => schema(field(0).toInt, field(1), field(2), field(3).toInt, field(4)))
        .toDF()

      // full name derivation
      val c = df.withColumn("name",expr("concat(first,' ', last)")).drop("first").drop("last")

      // Annual salary derivation
      val d = c.withColumn("annual_salary_rupee", expr("case when lower(currency) = 'dollar' then 12*75*monthly_salary " +
        "when lower(currency) = 'rupee' then 12*monthly_salary end"))
          .select("id", "name", "monthly_salary", "annual_salary_rupee", "currency")


      d.show()
      // writing to cassandra
      d.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "data", "keyspace" -> "employee"))
        .mode(SaveMode.Append)
        .save()
    }
  )

  ssc.start()
  ssc.awaitTermination()
}
