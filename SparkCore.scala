package src.main.scala.com.spark.DataFramePractices

import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkCore {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkCore")
    val context = new SparkContext(conf)
    val session = SparkSession.builder().config(conf).getOrCreate()
    val hdd = context.textFile("hdfs://localhost:54310/user/ubuntu/jpi.txt")
    //hdd.foreach(println)
    val error = hdd.filter(line  => line.contains("Error"))
   error.foreach(println)
    println("the line containing error "+error.count())
   val exception =  hdd.filter(line => line.contains("Exception"))
    println("the line containg exception "+exception.count())
    val unionErrorAndException = error.union(exception)
    println("the union count is "+unionErrorAndException.count())
    println("---------------------------------------------------------------------------------------------------")
    val unionHdd = unionErrorAndException.filter(line => line.contains("DAG"))

    var count = 0
    val cancelled = hdd.filter(line => line.contains("Stage cancelled"))
    print("the stage have been canceled "+cancelled.count +" times")


    cancelled.map(line => {

      line.concat("(thsese line forced to cancel)")
    }).saveAsTextFile("/home/ubuntu/Downloads/namenode/cancelationReason")

    hdd.distinct().saveAsTextFile("/home/ubuntu/Downloads/namenode/DistinctLine")
    hdd.map(line => (line,1)).reduceByKey((key1,key2) => key1+key2).saveAsTextFile("/home/ubuntu/Downloads/namenode/CountOfError")
    hdd.countByValue().foreach(println)
    val testWholeText = context.wholeTextFiles("/home/ubuntu/Downloads/namenode/TestWholeText")
    val filenames = testWholeText.keys.collect()
    println(testWholeText.count() +" are the number of files ")
    for(file <- filenames){
      println(s"location of the file -> $file")
    }



  }


}
