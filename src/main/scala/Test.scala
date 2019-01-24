import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j._
object Test{
  //def main(args:Array[String]):Unit ={
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf1=new SparkConf()
  conf1.setAppName("main")
  conf1.setMaster("local[*]")
  val sc=new SparkContext(conf1)
  val L=List(1,2,3,4,5)
  val Rdd1=sc.parallelize(L)
  val data=Rdd1.collect()
  data.foreach(println)
  //}
}
