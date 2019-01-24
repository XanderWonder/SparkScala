import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object AllSpark extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().set("spark.driver.allowMultipleContexts", "true").setMaster("local[*]").setAppName("main")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val rdd1 = sc.textFile("C:\\Users\\Admin\\Desktop\\Scala\\movies.txt")
//    val topLine = rdd1.first()
//    val rdd2 = rdd1.filter(x => x!= topLine)
//    val rdd3 = rdd2.map(x => {
//      val rec = x.split(",")
//      (rec(0).toInt, rec(1), rec(2), rec(3).toInt, rec(4))})
//    val rdd4 = rdd3.map(x => Row(x._1,x._2,x._3,x._4,x._5))
//    val schema = StructType(
//      StructField("Regno", IntegerType, true) ::
//        StructField("Name", StringType, true) ::
//        StructField("Subject", StringType, true) ::
//        StructField("Marks", IntegerType, true) ::
//        StructField("Client", StringType, true) ::
//        Nil
//    )
//
//    val DF = sqlContext.createDataFrame(rdd4).toDF(schema)
//    DF.show()
//        val df = sqlContext.createDataFrame(rdd3).toDF("REGNO","NAME","SUBJECT","MARKS","CLIENT")
//        df.show()
//        df.select(df("NAME") === "Shafeeq").show

    //val rdd1 = sc.textFile("C:\\Users\\Admin\\Desktop\\Scala\\movies.txt")
    //val rdd2 = rdd1.map(x => {
      //val rec = x.split("\t")
     // (rec(0).toInt,rec(1).toInt,rec(2),rec(3).toInt)
    //})
    //val movieDF = sqlContext.createDataFrame(rdd2).toDF("USER ID", "MOVIE ID", "RATING", "TIMESTAMP")
    //movieDF.filter(movieDF("RATING") = 5)
}
