import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Movies extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().set("spark.driver.allowMultipleContexts", "true").setAppName("test").setMaster("local[*]")

  def ddouble(A:Int):Int={A * A}
  val sc = new SparkContext(conf)
  val rddI = sc.textFile("C:\\Users\\Admin\\Desktop\\Scala\\movies.txt")

  val sqlContext = new SQLContext(sc)

  val MovieFilter = rddI.map(x => {
    val record = x.split("\t")
    (record(0).toInt, record(1).toInt, record(2).toInt, record(3).toLong)
  })

  val schema = StructType(
    StructField("movieId", IntegerType, true) ::
      StructField("watchAmount", IntegerType, true) ::
      StructField("movieRating", IntegerType, true) ::
      StructField("timeStamp", LongType, true) :: Nil)

  val abc = udf(ddouble(_),IntegerType)
  sqlContext.udf.register("abc",ddouble(_))

  val rdd2 = MovieFilter.map(x => Row(x._1, x._2, x._3, x._4))

  val df2= sqlContext.createDataFrame(rdd2,schema)

  var df3 =df2.filter(df2("movieRating")=== 5).groupBy("movieId").count

  df2.registerTempTable("testTable")
  sqlContext.sql("SELECT movieId as MovieID ,watchAmount as WatchAmount, abc(watchAmount) as Multiplier FROM testTable WHERE watchAmount > 200 LIMIT 20").show()


//  var highest_rated = df3.orderBy(df3("count").desc).first()
//  print(highest_rated)
}
