package main

import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File

object mapr {
  
  def main(args:Array[String]) {
    if(args.length<4)
    {
      System.err.println("usage:TwitterPopularTags<consumer key> < consumer scret>"+ "<access token> <access token scret >[<filters>]")
      System.exit(1)
    }
    
    //streaming by example
    
    StreamingExamples.setStreamingLogLevels()
    
    
    //passing our twitter token id and access token
     val Array(consumerKey,consumerSecret,accessToken,accessTokenSecret)=args.take(4)
     val filters = args.takeRight(args.length   - 4)
     
     //set system property so that twitter4j library is used by twiter stream
     //use them to generate oauth credentials
     
     System.setProperty("twitter4j.oauth.consumerkey",consumerKey)
     System.setProperty("twitter4j.oauth.consumerSecret",consumerSecret)
     System.setProperty("twitter4j.oauth.accessToken",accessToken)
     System.setProperty("twitter4j.oauth.accessTokenSecret",accessTokenSecret)
     
     val sparkConf = new SparkConf().setAppName("Sentiments").setMaster("local[2]")
     
     val ssc = new StreamingContext (sparkConf , Seconds(5))
     val stream =TwitterUtils.createStream(ssc,None,filters)  
     
     //input dstream transformation using flatmap
     val tags = stream.flatMap { status => status.getHashtTagEntities.map(_.getText)}
      
     //rdd transformation using sortBY
     tags.countByValue()  
     .foreachRDD {rdd =>
        val now = org.joda.time.DateTime.now()
        rdd
          .sortBy(_._2)
          .map(x=> (x,now))
          //saving directory 
          .saveAsTextFile(s"~/twitter/$now")
     }
     
     
     //dstream transformation using filter and map function
     val tweets = stream.filter { t=> 
       val tags = t.getText.split(" ").filter(_.startsWith("EDM")).map(_.toLowerCase) 
        tags.exists { x => true }
     }  
          
     val data = tweets.map { status =>
       
       val sentiment = SentimentAnalysisUtils.detectSentiment(status.getText)
       val tagss = status.getHashtagEntities.map(_.getText.toLowerCase)
       (status.getText,sentiment.toString,tagss.toStrings())
         }
     data.print()
  // saving our output
     data.saveAsTextFiles("~/twitterss","20000")
     
     ssc.start()
         ssc.awaitTermination()
         }
  }
  
  