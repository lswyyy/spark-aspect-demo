
import org.apache.spark.sql.SparkSession

object SparkAspectJTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.
      builder().
      master("local[1]").
      appName("test").
      enableHiveSupport().
      getOrCreate()

    spark.range(5).createTempView("t1")
    spark.range(10).createTempView("t2")

    spark.sql("select * from t1 left join t2 on t1.id = t2.id").show()
  }
}