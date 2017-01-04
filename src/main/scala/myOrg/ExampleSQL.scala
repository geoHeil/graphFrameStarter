package myOrg

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExampleSQL extends App {

  val conf: SparkConf = new SparkConf()
    .setAppName("exampleSQL")
    .setMaster("local[*]")
    .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "1G")
    .set("spark.kryoserializer.buffer", "100m")

  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val df = Seq(
    (0, "A", "B", "C", "D"),
    (1, "A", "B", "C", "D"),
    (0, "d", "a", "jkl", "d"),
    (0, "d", "g", "C", "D"),
    (1, "A", "d", "t", "k"),
    (1, "d", "c", "C", "D"),
    (1, "c", "B", "C", "D")
  ).toDF("TARGET", "col1", "col2", "col3TooMany", "col4")

  df.show

  // now using sql
  df.createOrReplaceTempView("mydf")
  spark.sql(
    """
      |SELECT * FROM mydf
    """.stripMargin
  ).show

  val homeDir = System.getProperty("user.home");
  var path = homeDir + File.separator + "neverpayer" + File.separator
  path = path.replaceFirst("^~", System.getProperty("user.home"))

  //  val rawDf = readCsv(spark, path + "pathToFile")

  spark.stop

  def readCsv(spark: SparkSession, inputPath: String): DataFrame = {
    //    import spark.implicits._
    spark.read.
      option("header", "true")
      .option("inferSchema", true)
      .option("charset", "UTF-8")
      .option("delimiter", ";")
      .csv(inputPath)
      .withColumn("col1", $"col1".cast("Date"))
      .withColumnRenamed("col2", "colAAA")
  }

}
