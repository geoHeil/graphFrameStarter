package myOrg.sparklingGraph

import myOrg.sparklingGraph.EigenvectorUtils._
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * adopted from https://github.com/sparkling-graph/sparkling-graph
  */
object FraudCentrality extends VertexMeasure[Double]{

  /**
    * Generic Fraudulence Centrality computation method, should be used for extensions, computations are done until @continuePredicate gives true
    * @param graph - computation graph
    * @param vertexMeasureConfiguration - configuration of computation
    * @param continuePredicate - convergence predicate
    * @param num - numeric for @ED
    * @tparam VD - vertex data type
    * @tparam ED - edge data type
    * @return graph where each vertex is associated with its fraudulence score
    */
  def computeFraudulence[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],
                                                  vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED],
                                                  continuePredicate:ContinuePredicate=convergenceAndIterationPredicate(1e-6))(implicit num:Numeric[ED])={
    val numberOfNodes=graph.numVertices
    val startingValue=1.0/numberOfNodes
    var computationGraph=graph.mapVertices((vId,data)=>startingValue)
    var iteration=0
    var oldValue=0d
    var newValue=0d
    val socialDepthDecay = 0.8

    // TODO unsure why originally this stream was used for / dad code?
//    Stream.from(0).map((iteration)=>{
//
//    })
    while(continuePredicate(iteration,oldValue,newValue)||iteration==0){
      // TODO find out what type of class should be passed to aggregate messages i.e. if double is enough, or if graphFraud.aggregateMessages[(Int, (String, Int))] as a full vertex (id, (name, fraud)) is required
      val iterationRDD=computationGraph.aggregateMessages[Double]( // where is the id of the final output if only the score is shred?
        sendMsg = context=>{
          context.sendToDst(num.toDouble(context.attr)*context.srcAttr)
          context.sendToSrc(0d)
          // I want to decay fraudulence scores further away by a factor, directness should be considered
          // as deliberate interaction with fraudulent vertex should be scored differently
          // than just being contacted by a fraudulent vertex
          // I think multiply by socialDepthDecay i.e. 0.8 for each iteration is fine
          if(vertexMeasureConfiguration.treatAsUndirected){
            context.sendToSrc(num.toDouble(context.attr)*context.dstAttr)
            context.sendToDst(0d)
          }
        },
        // assuming fraud field is binary, an average works just fine as the aggregation method
        mergeMsg = (a,b)=>a+b)
      // TODO find out if normalization is required / really useful for my modification, but in the end this is used
      // to generate a value [0, 1] i.e. sort of a probability which should be just  fine for me
      val normalizationValue=Math.sqrt(iterationRDD.map{case (vId,e)=>Math.pow(e,2)}.sum())
      computationGraph=computationGraph.outerJoinVertices(iterationRDD)((vId,oldValue,newValue)=>if(normalizationValue==0) 0 else newValue.getOrElse(0d)/normalizationValue).cache()
      oldValue=newValue
      newValue=computationGraph.vertices.map{case (vId,e)=>e}.sum()/numberOfNodes
      iterationRDD.unpersist()
      iteration+=1
    }
    computationGraph
  }
  /**
    * Computes Fraudulency Centrality for each vertex in graph
    * @param graph - computation graph
    * @param vertexMeasureConfiguration - configuration of computation
    * @param num - numeric for @ED
    * @tparam VD - vertex data type
    * @tparam ED - edge data type
    * @return graph where each vertex is associated with its fraudulence score
    */
  override def compute[VD:ClassTag, ED:ClassTag](graph: Graph[VD, ED],vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED])(implicit num:Numeric[ED]): Graph[Double, ED] = computeFraudulence(graph,vertexMeasureConfiguration)
  // TODO find out what implicit num:Numeric[ED] is in my case
}
