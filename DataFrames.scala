import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}

object DataFrames{

  def main(args: Array[String]): Unit = {

    val conf  = new SparkConf().setMaster("local").setAppName("DataFrames")
    val context = new SparkContext(conf)
    val dem =  List((1,"spark",3000),(2,"java",1500),(3,"hive",500))
val pl = context.parallelize(dem)
     val session = SparkSession.builder().config(context.getConf).getOrCreate()
    val dff = session.read.json("hdfs://localhost:54310/user/ubuntu/jpi.json")
   /*  dff.show()
   dff.printSchema()
*/
    dff.createOrReplaceTempView("dff")
    val result = session.sql("select country,count(1) as cnt from dff group by country order by cnt")
    result.show()
     dff.printSchema()
    dff.groupBy("Country").count().collect().foreach(println)
  }
}