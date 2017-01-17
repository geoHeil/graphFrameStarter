package myOrg

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
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

  val v = spark.createDataFrame(List(
    ("a", "Alice", 1),
    ("b", "Bob", 0),
    ("c", "Charlie", 0),
    ("d", "David", 0),
    ("e", "Esther", 0),
    ("f", "Fanny", 0),
    ("g", "Gabby", 0)
  )).toDF("id", "name", "fraud")
  // Edge DataFrame
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
  // Create a GraphFrame
  val g = GraphFrame(v, e)

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


//  val pw = new java.io.PrintWriter("myGraph.gexf")
//  pw.write(toGexf(g.toGraphX))
//  pw.close

  spark.stop

  // TODO this does not keep the required edge information e.g. type of edge
  // maybe look at R socialMediaLab package for help
  def toGexf[VD, ED](g: Graph[VD, ED]): String =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"
}

