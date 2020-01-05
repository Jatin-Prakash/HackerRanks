package src.main.scala.com.spark.DataFramePractices

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import java.io.File

import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.FilterQuery



object TwitterAnalysis {





    def main(args: Array[String]) {

      val logs = Logger.getRootLogger().getAllAppenders.hasMoreElements
      if(!logs){
        Logger.getRootLogger.setLevel(Level.WARN)
      }
      //StreamingExamples.setStreamingLogLevels()
      //Passing our Twitter keys and tokens as arguments for authorization
      val z = new Array[String](1)
      z(0) = "@realDonaldTrump"

      val filters = z

      // Set the system properties so that Twitter4j library used by twitter stream
      // Use them to generate OAuth credentials
      System.setProperty("twitter4j.oauth.consumerKey", "DrQocmWOD9SKanWL1VDpEcqzZ")

      System.setProperty("twitter4j.oauth.consumerSecret", "7RF2dWJQOcrvXmcOjlCZvQJhPFVwiAxUXmFVL0x1Xx18zMHZSb")

      System.setProperty("twitter4j.oauth.accessTokenSecret", "ZdQrJqprE8WXICEAg9jkNakvI6bPWNALGbmlSN9dv90Er")

      System.setProperty("twitter4j.oauth.accessToken", "511914336-S4TNMMOSCtNlrFG11cRfu4zHjlcamXvjOma7b1oB")

      val query = new FilterQuery()
      query.track("@realDonaldTrump")
      val opQuery = Option(query)
      val sparkConf = new SparkConf().setAppName("TwitterAnalysis").setMaster("local")
      val ssc = new StreamingContext(sparkConf,Seconds(2))
      val filteredStream = TwitterUtils.createFilteredStream(ssc,None,opQuery)
     // val stream = TwitterUtils..createStream(ssc, None, filters)
      filteredStream.transform(rdd => {

        null
      })
filteredStream.foreachRDD(statusRDD=>
  {
  statusRDD.foreach(status => {
  if(StringUtils.isNotEmpty(status.getText))
  println("this is the status "+status)
  }

  )
    statusRDD.saveAsTextFile("/home/ubuntu/Downloads/namenode/TestWholeText/status.txt")

  }

)
      //Input DStream transformation using flatMap
     // filteredStream.print()
      ssc.start()
      ssc.awaitTermination()
    }


}
