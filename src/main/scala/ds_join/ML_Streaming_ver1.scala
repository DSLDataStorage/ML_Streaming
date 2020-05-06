package ds_join

import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.sql._
import org.mongodb.scala._
import org.mongodb.scala.Document._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.bson._

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


/*Complile*/
/*
1. for shell : ./bin/spark-shell --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/REVIEPreferred"\
--packages org.mongodb.spark:mongo-spark-connector_2.11:2.1.0 /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

./bin/spark-shell --packages org.mongodb.scala:mongo-scala-driver_2.11:2.0.0 org.mongodb.scala:mongo-scala-bson_2.11:2.0.0 org.mongodb:bson.0.0/home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

2. for submit : ./bin/spark-submit --class ds_join.DS_SimJoin_stream --master $master /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar $num 

*/


/*check it is partitioner right?*/
object ML_Streaming_ver1{
      /* Local function */ 
  def CalculateH1 ( l: Int, threshold: Double ): Int = {
    Math.floor ( (1 - threshold) * l / threshold + 0.0001).toInt + 1
  }

  def segNum(s: String, n: Int): Int = {
    val hash = s.hashCode % n
    //println(s"segNum: " + s.toString + " , " + hash.toString)
    if (hash >= 0) {
      hash + 1
    } else {
      hash + n + 1
    }
  }

  def createInverse(ss1: String,
                                 group: Array[(Int, Int)],
                                 threshold: Double
                                ): Array[(String, Int, Int)] = {
    {
      val ss = ss1.split(" ").filter(x => x.length > 0)
      val range = group.filter(
        x => (x._1 <= ss.length && x._2 >= ss.length)
      )

      

      val sl = range(range.length-1)._1
      val H = CalculateH1(sl, threshold)

      for (i <- 1 until H + 1) yield {
        val s = ss.filter(x => {segNum(x, H) == i})
        if (s.length == 0) {
          Tuple3("", i, sl)
        } else if (s.length == 1) {
          Tuple3(s(0), i, sl)
        } else {
          Tuple3(s.reduce(_ + " " + _), i, sl)
        }
      }
    }.toArray
  }
  def sort2(xs: Array[(Int, ((String, String), Boolean, Long, Long))]): Array[(Int, ((String, String), Boolean, Long, Long))] = {
    if (xs.length <= 1) {
      xs
    } else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        sort2(xs.filter(s => (pivot._1 > s._1))),
        xs.filter(s => (pivot._1 == s._1)),
        sort2(xs.filter(s => (pivot._1 < s._1)))
      )
    }
  }
  def sort3(xs: Array[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))]): Array[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))] = {
    if (xs.length <= 1) {
      xs
    } else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        sort3(xs.filter(s => (pivot._1 > s._1))),
        xs.filter(s => (pivot._1 == s._1)),
        sort3(xs.filter(s => (pivot._1 < s._1)))
      )
    }
  }
  def sort4(xs: Array[(Long,(Int, ((String, String), Boolean, Long, Long)))]): Array[(Long,(Int, ((String, String), Boolean, Long, Long)))] = {
    if (xs.length <= 1) {
      xs
    } else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        sort4(xs.filter(s => (pivot._1 > s._1))),
        xs.filter(s => (pivot._1 == s._1)),
        sort4(xs.filter(s => (pivot._1 < s._1)))
      )
    }
  }  
  def sort(xs: Array[String]): Array[String] = {
    if (xs.length <= 1) {
      xs
    } else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        sort(xs filter (pivot >)),
        xs filter (pivot ==),
        sort(xs filter (pivot <))
      )
    }
  }

  def sortByValue(x: String): String = {
    sort(x.split(" ")).reduce(_ + " " + _)
  }
    def calculateOverlapBound2(t: Float, xl: Int, yl: Int): Int = {
    (Math.ceil((t / (t + 1)) * (xl + yl)) + 0.0001).toInt
  }

  def verify2(x: Array[(Array[Int], Array[Boolean])], // query
                     y0: ((String, String), Boolean, Long, Long), //index
                     threshold: Double,
                     pos: Int, xLength: Int,
                     multiGroup: Broadcast[Array[(Int, Int)]]
                    ): Boolean = {
   // println(s"enter verification, pos: ${pos}, xLength: ${xLength}, yLength: ${yLength}")
    

    var t0 = System.currentTimeMillis
    val y = createInverse(sortByValue(y0._1._1), multiGroup.value, threshold )
                    .map(x => {
                      if(x._1.length > 0){
                      (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())
                       }else {
                      (Array[Int](), Array[Boolean]())
                      }
                   })
    /* y : Array[(Array[Int], Array[Boolean])] */
    val yLength = y.map(x => x._1.length)
      .reduce(_ + _)
    var t1 = System.currentTimeMillis

    //println("time|DIMA|verify2(y, yLength): " + (t1 - t0) + " ms")

    val overlap = calculateOverlapBound2(threshold.asInstanceOf[Float], xLength, yLength)

    var currentOverlap = 0
    var currentXLength = 0
    var currentYLength = 0
    for (i <- 0 until x.length) {
      var n = 0
      var m = 0
      var o = 0
      while (n < x(i)._1.length && m < y(i)._1.length) {
        if (x(i)._1(n) == y(i)._1(m)) {
          o += 1
          n += 1
          m += 1
        } else if (x(i)._1(n) < y(i)._1(m)) {
          n += 1
        } else {
          m += 1
        }
      }
      currentOverlap = o + currentOverlap
      currentXLength += x(i)._1.length
      currentYLength += y(i)._1.length
      val diff = x(i)._1.length + y(i)._1.length - o * 2
      val Vx = {
        if (x(i)._2.length == 0) {
          0
        } else if (x(i)._2.length == 1 && !x(i)._2(0)) {
          1
        } else {
          2
        }
      }
      val Vy = {
        if (y(i)._2.length == 0) {
          0
        } else if (y(i)._2.length == 1 && !y(i)._2(0)) {
          1
        } else {
          2
        }
      }
      if (i + 1 < pos) {
        if ( diff < Vx || diff < Vy) {
          //println(s"diff : ${diff}, Vx :${Vx}, Vy : ${Vy}")
          //println(s"i:$i, overlap")

          return false
        }
      }
      if (currentOverlap + Math.min((xLength - currentXLength),
        (yLength - currentYLength)) < overlap) {
        /*
        println(s"i:$i, currentOverlap:$currentOverlap, " +
          s"xLength: $xLength, yLength: $yLength, currentXLength: $currentXLength, " +
          s"currentYLength: $currentYLength, overlap: $overlap, prune")
        */
        return false
      }
    }
    if (currentOverlap >= overlap) {
      return true
    } else {
      //println(s"finalOverlap:$currentOverlap, overlap: $overlap, false")
    
      return false
    }
  }

  def compareSimilarity2(
    query: ((Int, String, Array[(Array[Int], Array[Boolean])])
      , Boolean, Array[Boolean], Boolean, Int),
    index: ((String, String), Boolean, Long, Long),
    multiGroup: Broadcast[Array[(Int, Int)]], 
    threshold: Double): Boolean = {

    val pos = query._5
    val query_length = query._1._3
      .map(x => x._1.length)
      .reduce(_ + _)


    if (index._2) { //
      if (!query._2 && query._3.length > 0 && query._3(0)) {
          verify2(query._1._3, index, threshold, pos,
            query_length, multiGroup)
      } else {
        false
      }
    } else {
      verify2(query._1._3, index, threshold, pos,
        query_length, multiGroup)
    }
  }

  def main(args: Array[String]){
    
      /*Initialize variable*/
      var conf = new SparkConf().setAppName("ML_Streaming_ver1")
      var sc = new SparkContext(conf)
      var sqlContext = new SQLContext(sc)
      val ssc = new StreamingContext(sc, Milliseconds(6000)) // 700
      val stream = ssc.socketTextStream("192.168.0.15", 9999)
      val stream2 = ssc.socketTextStream("192.168.0.15", 9998)
      val stream3 = ssc.socketTextStream("192.168.0.15", 9997)
      val stream4 = ssc.socketTextStream("192.168.0.15", 9996)

      //val stream1 = stream.union(stream2).union(stream3).union(stream4)
      var AvgStream:Array[Long] = Array()

      val partition_num:Int = 8
      val threshold:Double = 0.8  // threshold!!!!!!!
      val alpha = 0.95
      var minimum:Int = 0
      var topDegree = 0
      var hashP = new HashPartitioner(partition_num)
      var streamingIteration = 1
      var latencyIteration = 1

      var cachingWindow = 1
      var pCachingWindow = 1
      var ppCachingWindow = 1
      var pppCachingWindow = 1
      var sCachingWindow = 1
      var sCachingWindow_pre = 1
      var sCachingWindow_preTime: Long = 0
      var sCachingWindow_time: Long = 0
      var alphaValue: Long = 215
      val checkoutval = 10 //

      var enableCacheCleaningFunction = true
      var isPerformed_CC_PrevIter = false

      var delCacheTimeList: List[Int] = null

      var currCogTime: Long = 0
      var currCacheTime: Long = 0
      var currDBTime: Long = 0
      var currStreamTime: Long = 0

      var pCogTime: Long = 0
      var ppCogTime: Long = 0
      var pppCogTime: Long = 0

      var pCacheTime: Long = 0
      var ppCacheTime: Long = 0
      var pppCacheTime: Long = 0
      var ppppCacheTime: Long = 0

      var pDBTime: Long = 0
      var ppDBTime: Long = 0
      var pppDBTime: Long = 0

      var pIterationTime: Long = 0
      var ppIterationTime:Long = 0
      var pOutputCount: Long = 0

      var missedKeysCount: Long = 0
      var pMissedKeysCount: Long = 0
      var ppMissedKeysCount: Long = 0

     // var queryRDD:org.apache.spark.rdd.RDD[(String, String)] = null
      var cacheTmp: org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean, Long, Long))] = null    // for cache update // signature, query cost, verify cost

      var LRU_RDD: org.apache.spark.rdd.RDD[(Int, Int)] = null
      var LRU_Tmp: org.apache.spark.rdd.RDD[(Int, Int)] = null      // for LRUKey update

      var globalCacheCount: Long = 0

      var streaming_data_all: Int = 0
      var time_all = 0

      /* FOR thread */
      var DB_PRDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean, Long, Long))] = null
      var removeList: org.apache.spark.rdd.RDD[(Int, Int)] = null
      var cachedPRDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean, Long, Long))] = null // (signature, ((inverse, origin), bool , query time, verify time))
      //var index:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null

      var multiGroup:Broadcast[Array[(Int, Int)]] = null
      var frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Long]] = null
      var partitionTable: Broadcast[scala.collection.immutable.Map[Int, Int]] = null


      
      var LRUKeyThread: Thread = null
      var CacheThread: Thread = null
      var RemoveListThread: Thread = null
      var EndCondition: Thread = null
      var HitThread:Thread = null


      var missedIPRDDCount: Long = 0

      var isEmpty_missedData = true
      var DB_count:Long = 0
      var cachedPRDDDataCount:Long = 0
      var cachedDataCount:Long = 0
      var query_count:Long = 0
      var hitdimacount:Long = 0
      var inputKeysRDD_count:Long = 0

      var hit_sum:Long = 0
      var query_sum:Long = 0
      var queryRDD_sum:Long = 0
      var cogroup_query_cache_sum:Long = 0
      var hit_dima_sum:Long = 0
      var inputKeysRDD_sum:Long = 0
      var LRU_sum:Long = 0
      var DB_get_sum:Long = 0
      var query_mapParition_sum:Long = 0
      var cwa_sum:Long = 0
      var miss_dima_sum:Long = 0
      var cached_sum:Long = 0
      var cache_time_sum:Long = 0
      var latency_sum:Long = 0
      var union_sum:Long = 0

      var hit_time:Long = 0
      var miss_time:Long = 0

      val data_num = args(0).toString
      //val db_coll_name = "Musical_Sig"+data_num
      val db_coll_name = "SF_sig"+data_num+"k"
      val coll_name = "mongodb://192.168.0.10:27018/dblp.SF_"+data_num+"k"
      val cache_name = "/home/user/Desktop/hongji/ref/dblp_sig1k.json"   
      var qlist = List[Int]()

      //change mongospark version = 2.2.6  to 2.1.0
      /*index collection*/
      val readConfig = ReadConfig(Map(
        "spark.mongodb.input.uri" -> coll_name,
        "spark.mongodb.input.readPreference.name" -> "primaryPreferred"      
       ))
      val load = MongoSpark.load(sc,readConfig)
      val preRDD = load.map( x => x.getString("title"))
      val dataRDD = preRDD.map(x => (x,x))

      /*
       --  Run DIMA BuildIndex 
          -- set indexedRDD from buildIndex and f , multigroup        
      */
      var buildIndexSig = BuildSig.main(sc, dataRDD, partition_num) // buildIndexSig = tuple4 ( index, f , multiGroup, sc )
      frequencyTable = sc.broadcast(buildIndexSig._2.collectAsMap())
      multiGroup = buildIndexSig._3
      sc = buildIndexSig._4
      minimum = buildIndexSig._5
      partitionTable = sc.broadcast(Array[(Int, Int)]().toMap)

      var frequencyTableV = frequencyTable.value

      //frequencyTable.value.foreach(println)//.mapValues(s => s.map(a => (a._2._2)).reduce(_+_)).foreach(println)

      var shashP = new SimilarityHashPartitioner(partition_num, partitionTable)

      /*cache collection*/

      var cache_file = sqlContext.read.json(cache_name)
      var rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = cache_file.rdd
      cachedPRDD = rows.map( x => (x(4).asInstanceOf[Long].intValue(),((x(1).toString,x(3).toString), x(2).toString.toBoolean, 5, 5))) // 100 is initial data( random )
      cachedPRDD = new SimilarityRDD(cachedPRDD.partitionBy(shashP), true).cache()


            
      
      /* build LRU_RDD using index(cache) data 
      if(enableCacheCleaningFunction){
         LRU_RDD = cachedPRDD.map( x => (x._1, 0)).partitionBy(shashP) /*   !!!!!!!!!!!!!!!here hashc???!!!!!!!!!!!!!!!!!  */
         LRU_RDD.cache().count()
      }
      */
      println("threshold : "+threshold)
      println("index coll: "+coll_name)
      println("cache coll: "+cache_name)
      println("sig_index coll: "+db_coll_name)
    
      /* Run DIMA Similarity Join 
      ===========================stream====================================
      */



      var start_total = System.currentTimeMillis
      stream.foreachRDD({ rdd =>

        if(!rdd.isEmpty()){

          
          val tStart = System.currentTimeMillis
          var compSign = 1
          var zippedRDD:org.apache.spark.rdd.RDD[(Int, (((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int), ((String, String), Boolean, Long, Long), Boolean ))] = null
          var hit_dima_RDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean, Long, Long))] = null
          var outputCount: Long = 0

          println("\n\nStart|Stream num: " + streamingIteration)
       
          var input_file = sqlContext.read.json(rdd)
          var rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = input_file.select("title").rdd
          // rows.collect().foreach(println)
          var queryRDD = rows.map( x => (x(0).toString, x(0).toString)).filter(s => (s._1.length > 10))
          if(queryRDD.isEmpty) println("queryRDD.isEmpty")
          val query_hashRDD = queryRDD.map(x => (x._1.hashCode(), x._1))
          
          queryRDD = queryRDD.cache()
          query_count = queryRDD.count()

          println("data|qc|query_count : " + query_count)
          query_sum = query_sum + query_count

          CacheThread = new Thread(){
            override def run = {
              var enableCacheCleaningFunction_th = enableCacheCleaningFunction
              var streamingIteration_th = streamingIteration
              var isEmpty_missedData_th = isEmpty_missedData // first iter = true
              var frequencyTable_filter = frequencyTableV
              var threshold:Long = 0
              var removeTop = false
              var clean = false 

              

              var t0 = System.currentTimeMillis
              //println("DB_PRDD.partitioner: "+DB_PRDD.partitioner)    //Hash
              //println("cachedPRDD.partitioner: "+cachedPRDD.partitioner)    //Hash

              if(isEmpty_missedData_th == true){
                cacheTmp = cachedPRDD
              }else{
                cacheTmp = cachedPRDD.union(DB_PRDD)

              }
              //println("cacheTmp.partitioner: "+cacheTmp.partitioner)

              if(hit_time > miss_time ) clean = true
              else clean = false

              if( clean == true ) { 
                var diff = hit_time - miss_time
                println("diff : "+diff)
                cacheTmp = cacheTmp.mapPartitions({iter =>
                  var preSort = iter.toArray
                  var indexSort = sort4(preSort.map(x => (x._2._3, x))).map( x=> (x._2)) // sort by query cost
                  var ilen = indexSort.size
                  var i = -1
                  var query_sum:Long = 0
                  var xi_sum:Long = 0
                  while(diff/2 - (query_sum)/2  > xi_sum && ilen > i ){
                    i += 1
                    var xi = indexSort(i)

                    var Tquery = xi._2._3
                    var Txi = xi._2._4
                    query_sum += Tquery
                    xi_sum += Txi

                    println(s"Tquery : ${Tquery} , Txi : ${Txi}, query_sum : ${query_sum}, xi_sum : ${xi_sum}")
                  }
                  println("i : "+i)
                  var newIter = indexSort.slice(i, ilen)

                newIter.iterator
                  
                }, preservesPartitioning = true)
              }

              println("cache|cache threshold : "+threshold+", clean : "+clean+", removeTop : "+removeTop)

              

              if(streamingIteration % checkoutval == 0){
                println("=======Localchechpoint======")
                cacheTmp.localCheckpoint
              }

              var test1 = System.currentTimeMillis
              cachedDataCount = cacheTmp.cache.count // check cache cout
              var test2 = System.currentTimeMillis
              println("time|6|create test1: " + (test2 - test1) + " ms")
              
              

              println("data|c|cached count(after union): " + cachedDataCount)  
              cached_sum = cached_sum + cachedDataCount
                    
              cachedPRDD.unpersist()
              DB_PRDD.unpersist()
              cachedPRDD = cacheTmp
             
              var t1 = System.currentTimeMillis
              println("time|6|create cachedPRDD(currCacheTime): " + (t1 - t0) + " ms")
              cache_time_sum = cache_time_sum + currCacheTime           
              currCacheTime = t1 - t0

            }
          }// CacheThread END

          CacheThread.start
          
          var t0 = System.currentTimeMillis 
          
          var queryForIndex = new SimilarityRDD(queryRDD.map(x => (sortByValue(x._1), x._2))
                  .map(x => ((x._1.hashCode, x._2, x._1),
                   DimaJoin.partition_r(
                       x._1, frequencyTable, partitionTable, minimum, multiGroup,
                         threshold, alpha, partition_num, topDegree
                     )))
                  .flatMapValues(x => x)
                  .map(x => { ((x._1._1, x._1._2, x._2._1), x._2._2)})
                  .flatMapValues(x => x)
                  .map(x => { (x._2._1, (x._1, x._2._2, x._2._3, x._2._4, x._2._5))}).partitionBy(shashP), true).cache() //x._2._1 => sig 
          //queryForIndex = queryForIndex.reduceByKey((a, b) => a) //x._2._1 => sig 

          queryForIndex.count()
          var t1= System.currentTimeMillis
          println("time|ex|queryForIndex : " + (t1 - t0) + " ms")
          queryRDD_sum = queryRDD_sum + (t1 - t0)

          queryRDD.unpersist()

          /* Thread */

          CacheThread.join()

          //cachedPRDD.collect().foreach(x => (print(x._2._3+", ")))

          var tt0 = System.currentTimeMillis 
          var ans1 = mutable.ListBuffer[(Int, (((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int), ((String, String), Boolean, Long, Long), Boolean ))]()
          
          zippedRDD = queryForIndex.zipPartitions(cachedPRDD, true){
            (leftIter, rightIter) => {
              val indexSort = sort2(rightIter.toArray)//rightIter.toArray//  // Array(cache signature)
              val querySort = sort3(leftIter.toArray)
              val ilen = indexSort.size
              val qlen = querySort.size
              println(s"qlent : ${qlen} , ilen : ${ilen}")
              var i = 0
              var q = 0
              while(ilen > i && qlen > q){
                if(q > 0 && querySort(q)._1 == querySort(q-1)._1){
                  q = q +1
                }else if(querySort(q)._1 < indexSort(i)._1){
                  if(i > 0 && querySort(q)._1 != indexSort(i-1)._1 ) {
                    ans1 += Tuple2(querySort(q)._1, (querySort(q)._2 , null, false))
                    println("no match : "+querySort(q)._1+" query : "+querySort(q)._2._1._2)
                  }
                  q = q + 1
                }else if(querySort(q)._1 > indexSort(i)._1) { 
                  i =  i + 1
                }else {
                      ans1 += Tuple2(querySort(q)._1, (querySort(q)._2 , indexSort(i)._2, true))
                      println("match : "+querySort(q)._1+" query : "+querySort(q)._2._1._2+", "+"cache : "+indexSort(i)._2._1._2)
                  i = i + 1
                 }
              }
              ans1.map(x => (x._1, (x._2._1, x._2._2, x._2._3))).iterator
            }
          }.cache()
         
          var zipCount = zippedRDD.filter(s => (s._2._3)).count()
          println("data|hc|zipCount count: " + zipCount )

          var tt1 = System.currentTimeMillis
          println("time|ex|zippedRDD.mapPartitions: " + (tt1 - tt0) + " ms")
          cogroup_query_cache_sum = cogroup_query_cache_sum + (tt1 - tt0) 

          var ansHit = mutable.ListBuffer[(Int, ((String, String), Boolean, Long, Long))]()
          val hitFuture = Future {
               var t0 = System.currentTimeMillis
              /* 
              hit_dima_RDD = zippedRDD.mapPartitions({ iter =>
                while(iter.hasNext){
                  var i = iter.next
                  if(i._2._2 != null){
                    if(compareSimilarity2(i._2._1, i._2._2, multiGroup, threshold ) ){
                      ansHit += Tuple2(i._1, i._2._2)

                    }
                  }
                }
                ansHit.map(x => (x._1, x._2)).iterator
                }, preservesPartitioning = true).cache()
                */
              
              hit_dima_RDD = zippedRDD.filter(x => (x._2._2 != null))
                          .filter(s => (compareSimilarity2(s._2._1, s._2._2, multiGroup, threshold)))
                          .mapValues(x => (x._2)).cache()
              
              var hitcount = hit_dima_RDD.count
              println("data|hc|hit data count : "+hitcount)
              hit_sum = hit_sum + hitcount
              var t1 = System.currentTimeMillis
              println("time|hit|hitFuture time: " + (t1 - t0) + " ms")   
              hit_dima_sum = hit_dima_sum + (t1 -t0)
              hit_time = t1 - t0 
              hitcount
          }

          hitFuture.onComplete {
            case Success(s) => println("hit future success")
            case Failure(e) => e.printStackTrace
          }

           val missedFuture = Future {

              /* for miss */
              var t0 = System.currentTimeMillis
              var missedRDD = zippedRDD.filter(s => (s._2._2 == null))
              //var misscount = missedRDD.count()
              //println("data|hc|misscount count: " + misscount ) 
              
              // miss ! co : (sig, ((query), (cache)))
              //var ans2 = mutable.ListBuffer[(Int, (String, String, Boolean))]() //(query sig, (query string, (inverse string, bool))
              var ans2 = mutable.ListBuffer[(Int, (((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int), ((String, String), Boolean, Long, Long), Boolean ))]()
              //println("missedRDD.partitioner: "+missedRDD.partitioner)    //Hash
              var mappedMRDD = missedRDD.mapPartitions({ iter =>

                  val client: MongoClient = MongoClient("mongodb://192.168.0.10:27018") //mongos server
                  val database: MongoDatabase = client.getDatabase("dblp")
                  val collection: MongoCollection[Document] = database.getCollection(db_coll_name)

                  var qlist = List[Int]()
                  var dbData:Array[(Int, ((String, String), Boolean, Long, Long))] = Array()
                  var q = 0
                  var k = 0

                  if(!iter.isEmpty){
                      var querySort = sort3(iter.map(x => (x._1, x._2._1)).toArray)

                      for(i <- 0 to querySort.size - 1){
                        qlist ::= querySort(i)._1
                      }

                      println(s"qlist size : ${qlist.size}")
                      val t0 = System.currentTimeMillis
                      var query = in("signature", qlist:_*)
                      var temp = collection.find(query) //.map(x => (x.getInteger("signature").toInt,((x.getString("inverse"), x.getString("raw")), x.getBoolean("isDel").toString.toBoolean))) //for Document
                          
                      var awaited = Await.result(temp.toFuture, scala.concurrent.duration.Duration.Inf)

                      val t1 = System.currentTimeMillis
                      for(data <- awaited){
                        dbData +:= (data.getInteger("signature").toInt,((data.getString("inverse"), data.getString("raw")), data.getBoolean("isDel").toString.toBoolean, (t1-t0)/(qlist.size).toLong, 0.toLong))  
                      }
                      
                      println("time|query|query time : " + (t1 - t0) + " ms with qlistsize : "+qlist.size) 

                        val indexSort = sort2(dbData)
                        while(indexSort.size > k && querySort.size > q){
                          println(s" ${q}, ${querySort(q)._1}, ${k}, ${indexSort(k)._1}")
                         if(q > 0 && querySort(q)._1 == querySort(q-1)._1){
                            q = q + 1
                         }else if(querySort(q)._1.toInt == indexSort(k)._1.toInt){
                          
                            val t2 = System.currentTimeMillis
                            val check = compareSimilarity2(querySort(q)._2, indexSort(k)._2, multiGroup, threshold)
                            val t3 = System.currentTimeMillis
                            if(check == true ){
                              ans2 += Tuple2(querySort(q)._1, (querySort(q)._2 , ((indexSort(k)._2._1._1, indexSort(k)._2._1._2), indexSort(k)._2._2, indexSort(k)._2._3 , (t3-t2)), true))
                            }else{
                              ans2 += Tuple2(querySort(q)._1, (querySort(q)._2 , ((indexSort(k)._2._1._1, indexSort(k)._2._1._2), indexSort(k)._2._2, indexSort(k)._2._3 , (t3-t2)), false))
                            }
                            k = k + 1
                          }else if(querySort(q)._1.toInt < indexSort(k)._1.toInt ){
                            q = q + 1
                          }else if(querySort(q)._1.toInt > indexSort(k)._1.toInt ){
                            k  = k + 1
                          }                 
                        }                         
                    }
                    client.close()
              
                  ans2.map(x => (x._1, (x._2._1, x._2._2, x._2._3))).iterator // (sig, (query._2, cache._2, isCache))
              }, preservesPartitioning = true).cache()
              //println("mappedMRDD.partitioner: "+mappedMRDD.partitioner)    //Hash
              var resultmissRDD = mappedMRDD.filter(s => (s._2._3)).cache()
              var resultmiss = resultmissRDD.count()
              println("data|hc|resultmiss dima(data) : "+resultmiss)

              DB_PRDD = mappedMRDD.mapValues(x => (x._2)).filter(x => (frequencyTable.value.getOrElse((x._1, x._2._2), 0.toLong) < 50 )).cache()
              var DB_get = DB_PRDD.count
              println("data|hc|DB_get data : "+ DB_get)
              DB_get_sum = DB_get_sum + DB_get

              isEmpty_missedData = false

              var t1 = System.currentTimeMillis
              println("time|ex|missedRDD.mapPartitions: " + (t1 - t0) + " ms")
              query_mapParition_sum = query_mapParition_sum +  (t1 - t0)
              miss_time = (t1 - t0)
              
              t0 = System.currentTimeMillis
              
              var missResult = resultmissRDD.mapValues(x => (x._2))
              println("missResult.partitioner: "+missResult.partitioner)    //Hash
              println("hit_dima_RDD.partitioner: "+hit_dima_RDD.partitioner)    //Hash
              var unionResult = hit_dima_RDD.union(missResult)
              outputCount = unionResult.count()
              println("data|hc|outputCount total(data) : "+outputCount)

              t1 = System.currentTimeMillis
              println("time|union time: " + (t1 - t0) + " ms")
              union_sum = union_sum +  (t1 - t0)

              zippedRDD.unpersist()
              queryForIndex.unpersist()
              mappedMRDD.unpersist()
              hit_dima_RDD.unpersist()
              resultmissRDD.unpersist()
          }

         // LRUKeyThread.start()

          var ct0 = System.currentTimeMillis
          val n =Await.result(missedFuture, scala.concurrent.duration.Duration.Inf)
          var ct1 = System.currentTimeMillis
          println("time|fu|missedFuture time: " + (ct1 - ct0) + " ms")

          
          /* ------- main ------*/
            
          rdd.unpersist()

          //DB_PRDD.unpersist()

          val tEnd = System.currentTimeMillis
          currStreamTime = tEnd - tStart
          println("time|8|latency: " + currStreamTime + " ms")
          if(( tEnd - start_total) > 600000 ) {
            println(" + latency ")
            latency_sum = latency_sum + currStreamTime
            latencyIteration = latencyIteration + 1
          }
         
          streamingIteration = streamingIteration + 1

          pppCogTime = ppCogTime
          ppCogTime = pCogTime
          pCogTime = currCogTime

          pppDBTime = ppDBTime
          ppDBTime = pDBTime
          pDBTime = currDBTime

          ppppCacheTime = pppCacheTime        
          pppCacheTime = ppCacheTime
          ppCacheTime = pCacheTime
        
          ppIterationTime = pIterationTime
        
          ppMissedKeysCount = pMissedKeysCount

          ppCachingWindow = pCachingWindow
          pCachingWindow = cachingWindow
        
          pCacheTime = currCacheTime
        
          pOutputCount = outputCount
          pIterationTime = currStreamTime
          pMissedKeysCount = missedKeysCount
        
          streaming_data_all = streaming_data_all + outputCount.toInt
          println("data|all|streaming data all: " + streaming_data_all)

          if(( tEnd - start_total) > 1800000 ){
            ssc.stop()
          }

        }
      })

      ssc.start()
      ssc.awaitTermination()
      var end_total = System.currentTimeMillis
      var total_time = end_total - start_total

      println("\n\n======(DS)Streaming average log=====\n")
      println("> total streaming iteration : "+streamingIteration)
      println("data|query_sum: " + query_sum/streamingIteration)
      println("time|build queryRDD_sum: "+queryRDD_sum/streamingIteration+" ms")
      println("time|cogroup_query_cache_sum: "+cogroup_query_cache_sum/streamingIteration+" ms")   
      println("data|hit sum: " + hit_sum/streamingIteration)
      println("time|hit_dima_sum: "+hit_dima_sum/streamingIteration+" ms")
      println("data|inputKeysRDD_sum: " + inputKeysRDD_sum/streamingIteration)
      println("time|LRU_sum: "+LRU_sum/streamingIteration+" ms")
      println("data|DB_get_sum: " + DB_get_sum/streamingIteration)
      println("time|query_mapParition_sum: "+query_mapParition_sum/streamingIteration+" ms")
      println("data|cwa_sum: " + cwa_sum/streamingIteration)
      println("time|miss_dima_sum: "+miss_dima_sum/streamingIteration+" ms")
      println("data|cached_sum: " + cached_sum/streamingIteration)
      println("time|cache_time_sum: "+cache_time_sum/streamingIteration+" ms")
      println("time|union_sum: "+union_sum/streamingIteration+" ms")
      println("data|streaming data all: " + streaming_data_all)
      println("time|latency_sum: "+latency_sum/latencyIteration+" ms")
      println("time|total time: "+total_time+" ms")
      println("\n=================================\n")

      



    }


 }