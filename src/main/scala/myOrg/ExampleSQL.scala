package myOrg

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
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

  // todo sort of get rid of this RDD stuff but otherwise:
  //java.lang.UnsupportedOperationException: No Encoder found for org.apache.spark.sql.Row
  //  - field (class: "org.apache.spark.sql.Row", name: "attr")

  // triplet of id, name, and fraud
  val vertices: RDD[(VertexId, (String, Int))] =
    spark.sparkContext.parallelize(Array(
      (1L, ("Alice", 1)),
      (2L, ("Bob", 0)),
      (3L, ("Charlie", 0)),
      (4L, ("David", 0)),
      (5L, ("Esther", 0)),
      (6L, ("Fanny", 0)),
      (7L, ("Gabby", 0)),
      (8L, ("Fraudster", 1))
    ))
  // Create an RDD for edges
  val edges: RDD[Edge[String]] =
    spark.sparkContext.parallelize(Array(
      Edge(1L, 2L, "call"),
      Edge(2L, 3L, "sms"),
      Edge(3L, 2L, "sms"),
      Edge(6L, 3L, "sms"),
      Edge(5L, 6L, "sms"),
      Edge(5L, 4L, "call"),
      Edge(4L, 1L, "call"),
      Edge(4L, 5L, "sms"),
      Edge(1L, 5L, "call"),
      Edge(1L, 8L, "call"),
      Edge(6L, 8L, "call")
    ))
  // Build the initial Graph
  val graphFraud = Graph(vertices, edges)

  //  graphFraud.edges.toDF().show
  //  graphFraud.vertices.toDF().show

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
    (currentDF, colName) =>
      {
        currentDF
          .withColumn("directFraudulencyScore", when($"fraud" === 1, 1)
            .otherwise(joinUDF(lit(colName._1), col(colName._1))))
      }
  }
    .orderBy("id")
    .show)

  // #################### with additional edge types
  // 2) a-> direct but considering different type of edge connection ##################################################
  val total2 = friends
    .groupBy($"a.id", $"e.relationship").count
    .withColumnRenamed("count", "totalCount")
  val fraudDirect2 = friends
    .filter($"b.fraud" === 1)
    .groupBy($"a.id").count
    .withColumnRenamed("count", "fraudCount")
  time(total2
    .join(fraudDirect2, Seq("id"), "left")
    .na.fill(0)
    .withColumn("fraudPercentage", $"fraudCount" / $"totalCount")
    .show)

  // now again without join
  // get rid of the join, perform different counts directly, but TODO make it work -> no will try pregel api
  // This does not work really well taking Array(String, String) as the key -> otherwise not unique values are handled
  // wrong. Will look into 2nd order friendship network first

  // ####################
  // connections of friends of friends 2nd order --------------------------------------------------------------------
  val friendsOfFriends: DataFrame = g.find("(a)-[e]->(b); (b)-[e2]->(c)")

  val totalF2 = friendsOfFriends
    .groupBy($"a.id").count
    .withColumnRenamed("count", "totalCount")
  val fraudDirectF2 = friendsOfFriends
    .filter("(b.fraud = 1) OR (c.fraud = 1)")
    .groupBy($"a.id").count
    .withColumnRenamed("count", "fraudCount")
  // lets try it with join first, but already for this minimal dataset it is quite slow
  // this does not weigh follower further away with a smaller fraud score TODO create a weith here (or at least in pregel api)
  println("friends of friends")
  time(totalF2
    .join(fraudDirectF2, Seq("id"), "left")
    .na.fill(0)
    .withColumn("fraudPercentage", $"fraudCount" / $"totalCount")
    .show)

  // finding fraud using stateful motifs TODO find out how to use stateful motifs here ################################
  // this is an example from the graph frames documentation. So far I could ont really use it (better) than what is up above

  // ########################################################################################################
  // now pregl message passing API: Message passing via AggregateMessages for graphFrames ##########################################
  val AM = AggregateMessages

  //  val msgToSrc = AM.dst("fraud")
  //  val msgToDst = AM.src("fraud")
  // multiply with weight (fraud far away is less important as fraud near the node)
  // to test use score of 1
  val fraudNeighbourWeight = 1.0
  // TODO how to add in conditions / total and percentage / weights / type of connection
  val msgToSrc: Column = when(AM.src("fraud") === 1, lit(fraudNeighbourWeight) * (lit(1) + AM.dst("fraud")))
  //.otherwise(AM.dst("fraud")))
  val msgToDst: Column = when(AM.dst("fraud") === 1, lit(fraudNeighbourWeight) * (lit(2) + AM.src("fraud")))
  // I am confused about the messages! why do I need to swap them?
  // how to prevent messages circulating forever?
  val agg = g.aggregateMessages
    .sendToSrc(msgToDst) // send destination user's fraud to source
    .sendToDst(msgToSrc) // send source user's fraud to destination
    .agg(sum(AM.msg).as("summedFraud")) // sum up fraud, stored in AM.msg column
  agg.show()

  // ########################################################################################################
  val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(spark.sparkContext, numVertices = 100).mapVertices((id, _) => id.toDouble)

  // Compute the number of older followers and their total age
  val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
    triplet => {
      // Map Function
      if (triplet.srcAttr > triplet.dstAttr) {
        // Send message to destination vertex containing counter and age
        triplet.sendToDst((1, triplet.srcAttr))
      }
    },
    // Add counter and age
    (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
  )
  // Divide total age by number of older followers to get average age of older followers
  val avgAgeOfOlderFollowers: VertexRDD[Double] =
    olderFollowers.mapValues((id, value) =>
      value match {
        case (count, totalAge) => totalAge / count
      })
  // Display the results
  avgAgeOfOlderFollowers.toDF().show
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

