package myOrg

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.StatCounter
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages

object ExampleSQL extends App {

  val logger: Logger = Logger.getLogger(this.getClass)

  val conf: SparkConf = new SparkConf()
    .setAppName("graphExample")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val v = spark.createDataFrame(List(
    ("a", "Alice", 1),
    ("b", "Bob", 0),
    ("c", "Charlie", 0),
    ("d", "David", 0),
    ("e", "Esther", 0),
    ("f", "Fanny", 0),
    ("g", "Gabby", 0),
    ("h", "Fraudster", 1)
  )).toDF("id", "name", "fraud")
  val e = spark.createDataFrame(List(
    ("a", "b", "call"),
    ("b", "c", "sms"),
    ("c", "b", "sms"),
    ("f", "c", "sms"),
    ("e", "f", "sms"),
    ("e", "d", "call"),
    ("d", "a", "call"),
    ("d", "e", "sms"),
    ("a", "e", "call"),
    ("a", "h", "call"),
    ("f", "h", "call")
  )).toDF("src", "dst", "relationship")
  val g = GraphFrame(v, e)

  // TODO use nicer method to write files
  // http://stackoverflow.com/questions/4604237/how-to-write-to-a-file-in-scala
  // or https://github.com/pathikrit/better-files
  // or https://github.com/scalaz/scalaz-stream
    val pw = new java.io.PrintWriter("myGraph.graphml")
    pw.write(toGraphML(g))
    pw.close

  // connections of direct friends
  val friends: DataFrame = g.find("(a)-[e]->(b)")
  // 1) a-> b, direct, b fraud
  // naive version ###############################################################################
  val total = friends.groupBy($"a.id").count.withColumnRenamed("count", "totalCount")
  val fraudDirect = friends.filter($"b.fraud" === 1).groupBy($"a.id").count.withColumnRenamed("count", "fraudCount")
  // lets try it with join first, but already for this minimal dataset it is quite slow
  time(total
    .join(fraudDirect, Seq("id"), "left").
    na.fill(0)
    .withColumn("fraudPercentage", $"fraudCount" / $"totalCount")
    .orderBy("id")
    .show)

  // gwithout join, but fairly complex. TODO this is quite some code overhead. Could this be simplified (by a lot)?
  // I know that the code handles multiple columns which is not really needed ... but anything else?
  val fraudGroupDirect = friends.groupBy($"a.id", $"b.fraud").count

  val exploded = explode(array(
    (Seq("id")).map(c =>
      struct(lit(c).alias("k"), col(c).alias("v"))): _*
  )).alias("level")

  val long = fraudGroupDirect.select(exploded, col("fraud"))

  // http://stackoverflow.com/questions/41445571
  val lookup = long.as[((String, String), Int)].rdd
    // You can use prefix partitioner (one that depends only on _._1)
    // to avoid reshuffling for groupByKey. Required only for distributed collection
    .aggregateByKey(StatCounter())(_ merge _.toDouble, _ merge _)
    .map { case ((c, v), s) => (c, (v, s)) }
    .groupByKey
    .mapValues(_.toMap)
    .collectAsMap
  val joinUDF = udf((newColumn: String, newValue: String) => {
    lookup.get(newColumn).flatMap(_.get(newValue))
      .fold(0.0) { case s: StatCounter => s.mean }
  })
  time(lookup.foldLeft(g.vertices) {
    (currentDF, colName) => {
      currentDF
        .withColumn("directFraudulencyScore", when($"fraud" === 1, 1)
          .otherwise(joinUDF(lit(colName._1), col(colName._1))))
    }
  }
    .orderBy("id")
    .show)

  // ########################################################################################################
  // now pregl message passing API: Message passing via AggregateMessages ##########################################
  //  val g: GraphFrame = examples.Graphs.friends
  val AM = AggregateMessages

  val msgToSrc = AM.dst("fraud")
  val msgToDst = AM.src("fraud")
  val agg = g.aggregateMessages
    .sendToSrc(msgToSrc)  // send destination user's fraud to source
    .sendToDst(msgToDst)  // send source user's fraud to destination
    .agg(sum(AM.msg).as("summedFraud"))  // sum up ages, stored in AM.msg column
  // TODO how to add in conditions / total and percentage / weights / type of connection
  time(agg.show)
  // ########################################################################################################

  // TODO access node via proper XML writer
  // maybe https://github.com/apache/tinkerpop/blob/4293eb333dfbf3aea19cd326f9f3d13619ac0b54/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/structure/io/graphml/GraphMLWriter.java is helpful
  // https://github.com/apache/tinkerpop/blob/4293eb333dfbf3aea19cd326f9f3d13619ac0b54/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/structure/io/graphml/GraphMLTokens.java
  /// TODO improve writer as outlined by https://github.com/sparkling-graph/sparkling-graph/issues/8 and integrate there
  def toGraphML(g: GraphFrame): String =
  s"""
     |<?xml version="1.0" encoding="UTF-8"?>
     |<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
     |         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     |         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
     |         http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
     |
     |  <key id="v_name" for="node" attr.name="name" attr.type="string"/>
     |  <key id="v_fraud" for="node" attr.name="fraud" attr.type="int"/>
     |  <key id="e_edgeType" for="edge" attr.name="edgeType" attr.type="string"/>
     |  <graph id="G" edgedefault="directed">
     |${
    g.vertices.map {
      case Row(id, name, fraud) =>
        s"""
           |      <node id="${id}">
           |         <data key = "v_name">${name}</data>
           |         <data key = "v_fraud">${fraud}</data>
           |      </node>
           """.stripMargin
    }.collect.mkString.stripLineEnd
  }
     |${
    g.edges.map {
      case Row(src, dst, relationship) =>
        s"""
           |      <edge source="${src}" target="${dst}">
           |      <data key="e_edgeType">${relationship}</data>
           |      </edge>
           """.stripMargin
    }.collect.mkString.stripLineEnd
  }
     |  </graph>
     |</graphml>
  """.stripMargin

  private def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed: " + ((t1 - t0).toDouble / 1000000000) + "sec")
    result
  }

  spark.stop

}

