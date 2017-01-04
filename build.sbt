name := "sparkMiniSample"
organization := "myOrg"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Xlint:missing-interpolator",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-dead-code",
  "-Ywarn-unused"
)

//The default SBT testing java options are too small to support running many of the tests
// due to the need to launch Spark in local mode.
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

lazy val spark = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark % "provided",
  "org.apache.spark" %% "spark-sql" % spark % "provided",
  "org.apache.spark" %% "spark-hive" % spark % "provided"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

assemblyMergeStrategy in assembly := {
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

test in assembly := {}

initialCommands in console :=
  """
    |import java.io.File
    |
    |import org.apache.spark.SparkConf
    |import org.apache.spark.sql.{DataFrame, SparkSession}
    |
    |val conf: SparkConf = new SparkConf()
    |    .setAppName("exampleSQL")
    |    .setMaster("local[*]")
    |    .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    |    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    |    .set("spark.kryoserializer.buffer.max", "1G")
    |    .set("spark.kryoserializer.buffer", "100m")
    |
    |  val spark: SparkSession = SparkSession
    |    .builder()
    |    .config(conf)
    |    .enableHiveSupport()
    |    .getOrCreate()
    |
    |  import spark.implicits._
  """.stripMargin

mainClass := Some("myOrg.ExampleSQL")
