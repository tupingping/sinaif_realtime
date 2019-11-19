package com.sinaif.sparkexample

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object test1{
  def main(args: Array[String]): Unit = {
    blackListFilter()
  }

  // 单词数量统计
  def wordCount(): Unit ={
    val conf = new SparkConf().setAppName("word count").setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(5))

    val lines = sc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word=>(word, 1))
    val wordcount = pairs.reduceByKey(_+_)

    wordcount.print()
    sc.start()
    sc.awaitTermination()

  }

  // 实时过滤黑名单用户
  def blackListFilter(): Unit ={
    val conf = new SparkConf().setAppName("blacklistfilter").setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(5))

    val blacklistRDD = sc.sparkContext.parallelize(Array(("tupp",true),("ccm", true)))

    val linesDStream = sc.socketTextStream("localhost",10000)

    linesDStream.map(
      l=>{
        val info = l.split(" ")
        (info(0), info.toList.tail.mkString(" "))
      }).transform(rdd=>{
      val joinRDD = rdd.leftOuterJoin(blacklistRDD)
      val filterRDD = joinRDD.filter(x=>x._2._2.isEmpty)
      filterRDD
    }).map(x=>(x._1,x._2._1)).print()

    sc.start()
    sc.awaitTermination()

  }

  // 基于缓存的wordcount，有状态的算子，需要checkpoint
  def wordCountBaseCache(): Unit = {
    val conf = new SparkConf().setAppName("word count base cache").setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(5))

    sc.checkpoint("")

    val lineDStream = sc.socketTextStream("localhost", 9999)
    val words = lineDStream.flatMap(l=>l.split(""))
    val wordCount = words.map(l=>(l,1))

    val rd = wordCount.updateStateByKey((values:Seq[Int], state:Option[Int])=>{
      var count = state.getOrElse(0)
      for(v<-values){
        count += v
      }

      Option(count)

    })

    rd.print()

    sc.start()
    sc.awaitTermination()

  }

  // 在线热点搜索词滑动统计,每隔2秒，统计最近5秒内，排名最靠前的3个搜索词及次数
  def windowandcount(): Unit ={
    val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(1))

    val lineDStream = sc.socketTextStream("localhost", 9999)

    lineDStream.flatMap(_.split(" ")).
      map((_,1)).
      reduceByKeyAndWindow((x:Int, y:Int)=>x+y, Seconds(5), Seconds(2)).transform(rdd =>{
       val info:Array[(String,Int)] = rdd.sortBy(_._2, false).take(3)
       val resultRDD = sc.sparkContext.parallelize(info)
       resultRDD
    }).map(x=>(x._1,x._2)).print()

    sc.start()
    sc.awaitTermination()
  }
}


