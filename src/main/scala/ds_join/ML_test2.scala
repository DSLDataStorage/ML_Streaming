package ds_join

import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.sql._
import org.mongodb.scala._
import org.mongodb.scala.Document._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.bson._
import org.apache.spark.sql.{Encoder, Encoders}

//import org.bson.Document

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.mutable

import scala.collection.mutable.Map

import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
//import org.apache.spark.mllib.regression.LinearRegressionModel
//import org.apache.spark.mllib.regression.LinearRegressionWithSGD
//import org.apache.spark.ml.classification.LogisticRegression

/*Complile*/
/*
1. for shell : ./bin/spark-shell --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/REVIEPreferred"\
--packages org.mongodb.spark:mongo-spark-connector_2.11:2.1.0 /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

./bin/spark-shell --packages org.mongodb.scala:mongo-scala-driver_2.11:2.0.0 org.mongodb.scala:mongo-scala-bson_2.11:2.0.0 org.mongodb:bson.0.0/home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

2. for submit : ./bin/spark-submit --class ds_join.DS_SimJoin_stream --master $master /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar $num 

*/

/*check it is partitioner right?*/
object ML_test2{
 
  def main(args: Array[String]){
     
      
      
      /*Initialize variable*/
      val conf = new SparkConf().setAppName("ML_test2")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val ssc = new StreamingContext(sc, Milliseconds(3000))
      val stream = ssc.socketTextStream("192.168.0.15", 9999)
    
      var AvgStream:Array[Long] = Array()

      val partition_num:Int = 8
      var hashP = new HashPartitioner(partition_num)
      var streamingIteration = 1
      var latencyIteration = 1

      val test_file = sqlContext.read.json("/home/user/Desktop/hongji/ref/test.json")
      var test_rows = test_file.select("reviewText", "overall").rdd
      var test_int_rows = test_rows.map(x => (x(1).toString.toDouble, Vectors.dense(x(0).toString.hashCode.toDouble)))//.toDF("overall", "reviewText")
          
      println("create model")
      var model = new StreamingLinearRegressionWithSGD_dsl()
      .setStepSize(0.1)
      .setNumIterations(50)
      .setRegParam(0.0)
      .setMiniBatchFraction(1.0)
      .setInitialWeights(Vectors.zeros(1))

      println("created model")

      val start_total = System.currentTimeMillis
   
      
       stream.foreachRDD({ rdd =>

        if(!rdd.isEmpty()){
          println("\n\nStart|Stream num: " + streamingIteration)


          var Start = System.currentTimeMillis
          var input_file = sqlContext.read.json(rdd)
          var rows = input_file.select("reviewText", "overall").rdd
          var int_rows = rows.map(x => (LabeledPoint(x(1).toString.toDouble, Vectors.dense(x(0).toString.hashCode.toDouble))))//.toDF("overall", "reviewText")
          var query_count = int_rows.cache().count()

          println("data|qc|query_count : " + query_count)

          /* model train */
          var tStart = System.currentTimeMillis
          model.trainOn_dsl(int_rows)
          var tEnd = System.currentTimeMillis
          println("time|train: "+(tEnd - tStart)+" ms")

          /* model test */
          tStart = System.currentTimeMillis
          var predic_count = model.predictOnValues_dsl(test_int_rows).count()//llect().foreach(println)
          println("data|pc|predictOnValues_dsl : " + predic_count)
          tEnd = System.currentTimeMillis

          println("time|prediction: "+(tEnd - tStart)+" ms")


          rdd.unpersist()
          int_rows.unpersist()

          var End = System.currentTimeMillis

          println("time|latency: "+(End - Start)+" ms")
          streamingIteration = streamingIteration + 1
          if(( tEnd - start_total) > 1800000 ){
            ssc.stop()
          }

        }
      })

      ssc.start()
      ssc.awaitTermination()
      var end_total = System.currentTimeMillis
      var total_time = end_total - start_total
      println("time|total_time: "+(total_time)+" ms")







    }


 }