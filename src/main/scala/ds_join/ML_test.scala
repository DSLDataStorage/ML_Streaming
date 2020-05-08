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

//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
//import org.apache.spark.mllib.regression.LinearRegressionModel
//import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.ml.classification.LogisticRegression

/*Complile*/
/*
1. for shell : ./bin/spark-shell --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/REVIEPreferred"\
--packages org.mongodb.spark:mongo-spark-connector_2.11:2.1.0 /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

./bin/spark-shell --packages org.mongodb.scala:mongo-scala-driver_2.11:2.0.0 org.mongodb.scala:mongo-scala-bson_2.11:2.0.0 org.mongodb:bson.0.0/home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

2. for submit : ./bin/spark-submit --class ds_join.DS_SimJoin_stream --master $master /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar $num 

*/

/*check it is partitioner right?*/
object ML_test{
 
  def main(args: Array[String]){
     
      
      
      /*Initialize variable*/
      val conf = new SparkConf().setAppName("ML_test")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._
    
      var AvgStream:Array[Long] = Array()

      val partition_num:Int = 8
      var hashP = new HashPartitioner(partition_num)
      var streamingIteration = 1
      var latencyIteration = 1


      var model:org.apache.spark.mllib.regression.LinearRegressionModel = null
      var start_total = System.currentTimeMillis
   
      

      val input_file = sqlContext.read.json("/home/user/Desktop/hongji/ref/test.json")
      var rows = input_file.select("reviewText", "overall")
      var int_rows = rows.map(x => (x(1).toString.toDouble, Vectors.dense(x(0).toString.hashCode.toDouble)))//.toDF("overall", "reviewText")

      var tStart = System.currentTimeMillis
      val lr = new LogisticRegression()
            .setMaxIter(1)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
            .setFeaturesCol("_2")
            .setLabelCol("_1")

      val lrmodel = lr.fit(int_rows)
     // val col = lrmodel.coefficientMatrix.numCols

      //println(s"Coefficients : ${lrmodel.coefficientMatrix.toString(col, Int.MaxValue)} Intercept : ${lrmodel.interceptVector}")

    // val Mdf =  sc.parallelize(lrmodel.coefficientMatrix.rowIter.toSeq)
    // Mdf.collect().foreach(println)

     var tEnd = System.currentTimeMillis

     println("time|train: "+(tEnd - tStart)+" ms")

     tStart = System.currentTimeMillis

     val test_file = sqlContext.read.json("/home/user/Desktop/hongji/ref/test.json")
     var test_rows = test_file.select("reviewText", "overall")
     var test_int_rows = test_rows.map(x => (x(1).toString.toDouble, Vectors.dense(x(0).toString.hashCode.toDouble)))//.toDF("overall", "reviewText")

     val predictions = lrmodel.transform(test_int_rows)

     predictions.select("_1", "_2", "prediction").show(30)

     tEnd = System.currentTimeMillis

     println("time|prediction: "+(tEnd - tStart)+" ms")


    /*   
          var outputCount: Long = 0

                
          var input_file = sqlContext.read.json("/home/user/Desktop/hongji/ref/test.json")
          var rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = input_file.select("reviewText", "overall").rdd
          //var split_rows = rows.map(line => LabeledPoint(line(1).toString.toDouble, Vectors.dense(line(0).toString.split(" ").map(x => x.hashCode).map(_.toDouble))))
          var split_rows = rows.map(line => LabeledPoint(line(1).toString.toDouble, Vectors.dense(line(0).toString.hashCode.toDouble)))
          val query_count = split_rows.cache().count()
          println("query count : "+query_count)


          var tS = System.currentTimeMillis
         // if(streamingIteration > 1) model = LinearRegressionModel.load(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
          model = LinearRegressionWithSGD.train(split_rows, 100, 0.01)
          var tE = System.currentTimeMillis 

          println("time|model train: "+(tE - tS)+" ms")

          
          tS = System.currentTimeMillis
          val valuesAndPreds = split_rows.map(point => (point.label, model.predict(point.features)))
          

          val MSE =  valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
          println(s"training Mean Squared Error $MSE")

          tE = System.currentTimeMillis

          println("time|model test: "+(tE - tS)+" ms")

          model.save(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
          
            
        

          val tEnd = System.currentTimeMillis

          println("time|latency: "+(tEnd - tStart)+" ms")


      var end_total = System.currentTimeMillis
      var total_time = end_total - start_total

      //m.foreach(x => println(x._1+", "+x._2))
      */




    }


 }