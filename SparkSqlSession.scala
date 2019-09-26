package src.main.scala.com.spark.DataFramePractices

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlSession {
  def main(args: Array[String]): Unit = {

    val conf  = new SparkConf().setMaster("local").setAppName("SparkSqlSession")
    val context = new SparkContext(conf)

    val session = SparkSession.builder().config(conf).getOrCreate()

    getDataframe(session)
  }

  def getDataframe(session : SparkSession): Unit = {
    val csvFile =  session.read.option("header","true").csv("/home/ubuntu/Downloads/namenode/TestWholeText/FL_insurance_sample.csv")
    //csvFile.show()
    csvFile.distinct().orderBy("statecode").show(50)
    csvFile.select("statecode","county").distinct().show(50)
    println( csvFile.filter("statecode = 'FL'" ).count())

  }
}
