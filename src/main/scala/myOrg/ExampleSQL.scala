package myOrg

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.graphframes.GraphFrame

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
    ("g", "Gabby", 0)
  )).toDF("id", "name", "fraud")
  val e = spark.createDataFrame(List(
    ("a", "b", "A"),
    ("b", "c", "B"),
    ("c", "b", "B"),
    ("f", "c", "B"),
    ("e", "f", "B"),
    ("e", "d", "A"),
    ("d", "a", "A"),
    ("a", "e", "A")
  )).toDF("src", "dst", "relationship")
  val g = GraphFrame(v, e)

  println(toGraphML(g))
  val pw = new java.io.PrintWriter("myGraph.graphml")
  pw.write(toGraphML(g))
  pw.close

  g.vertices.show
  g.edges.show

  g.inDegrees.show
  g.outDegrees.show
  g.degrees.show
  //  g.vertices.groupBy().max("fraud").show

  g.edges.filter("relationship = 'B'").count

  //  val g = examples.Graphs.friends
  val motifs: DataFrame = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
  motifs.show
  motifs.filter("b.fraud > 0").show

  val chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

  // Query on sequence, with state (cnt)
  //  (a) Define method for updating state given the next element of the motif.
  def sumCall(cnt: Column, relationship: Column): Column = {
    when(relationship === "A", cnt + 1).otherwise(cnt)
  }

  //  (b) Use sequence operation to apply method to sequence of elements in motif.
  //      In this case, the elements are the 3 edges.
  val condition = Seq("ab", "bc", "cd").
    foldLeft(lit(0))((cnt, e) => sumCall(cnt, col(e)("relationship")))
  //  (c) Apply filter to DataFrame.
  val chainWith2Friends2 = chain4.where(condition >= 2)
  chainWith2Friends2.show

  // TODO compute mentioned fraud values values
  // percentage of in and out degree for fraud for direct node and friends of friends

  // compute shortest pats to fraud

  // TODO access node
  // maybe https://github.com/apache/tinkerpop/blob/4293eb333dfbf3aea19cd326f9f3d13619ac0b54/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/structure/io/graphml/GraphMLWriter.java is helpful
  // https://github.com/apache/tinkerpop/blob/4293eb333dfbf3aea19cd326f9f3d13619ac0b54/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/structure/io/graphml/GraphMLTokens.java

  toGraphML(g)
  println(toGraphML(g))
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

  spark.stop

}

