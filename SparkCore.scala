package src.main.scala.com.spark.DataFramePractices

import java.io.StringReader
import com.datastax.spark.connector._
import com.opencsv.CSVReader
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkCore {

  val conf = new SparkConf().setMaster("local").setAppName("SparkCore")
  val context = new SparkContext(conf)
  val blankLine = context.accumulator(0)
  val lineWithWords = context.accumulator(0)

  def main(args: Array[String]): Unit = {

   val session = SparkSession.builder().config(conf).getOrCreate()
    val hdd = context.textFile("hdfs://localhost:54310/user/ubuntu/jpi.txt")
    //hdd.foreach(println)
    val error = hdd.filter(line  => line.contains("Error"))
   hdd.flatMap(wordCount).map(x => (x,1)).reduceByKey((x,y) =>x+y).foreach(println)

    println("the values of "+blankLine +" and "+lineWithWords)

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

    testWholeText.mapValues(line => line +"file contains "+line.length +" number of words").foreach(println)

    println("------------------------------------------------dealing with Semi Structure format----------------------------------")

    val insuranceRecord = context.textFile("/home/ubuntu/Downloads/namenode/TestWholeText/FL_insurance_sample.csv")
    insuranceRecord.map(record => {
      val reader = new CSVReader(new StringReader(record))
      reader.readNext()
    })


connectToCassandra(context)


  }


  def connectToCassandra(context: SparkContext ) = {

    lazy val data = context.cassandraTable("sparkdb","edwdata")
    data.foreach(println)
  }

  def wordCount(line : String) : Array[String] = {
println("started at wordCount method")
    if(line.contains(" ")){
      blankLine +=1
      Array[String]()
    }else{
        lineWithWords +=1
      line.split(" ")
    }

  }
}
