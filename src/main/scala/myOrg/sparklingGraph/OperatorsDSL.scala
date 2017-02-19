package myOrg.sparklingGraph

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object OperatorsDSL {

  implicit class DSL[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) {
    def fraudCentrality(vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED] = VertexMeasureConfiguration())(implicit num: Numeric[ED]) =
      FraudCentrality.compute(graph, vertexMeasureConfiguration)
  }

}
