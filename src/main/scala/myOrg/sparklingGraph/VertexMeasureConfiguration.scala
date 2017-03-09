package myOrg.sparklingGraph

import myOrg.sparklingGraph.IterativeComputation._

import scala.reflect.ClassTag

/**
  * Configuration of vertex measure alghoritms
  * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
  */
case class VertexMeasureConfiguration[VD: ClassTag, ED: ClassTag](bucketSizeProvider: BucketSizeProvider[VD, ED],
                                                                  treatAsUndirected: Boolean = false, considerEdgeType: Boolean = false)

object VertexMeasureConfiguration {

  def apply[VD: ClassTag, ED: ClassTag]() =
    new VertexMeasureConfiguration[VD, ED](wholeGraphBucket[VD, ED] _)

  def apply[VD: ClassTag, ED: ClassTag](treatAsUndirected: Boolean, considerEdgeType: Boolean) =
    new VertexMeasureConfiguration[VD, ED](wholeGraphBucket[VD, ED] _, treatAsUndirected, considerEdgeType)

  def apply[VD: ClassTag, ED: ClassTag](treatAsUndirected: Boolean, considerEdgeType: Boolean, bucketSizeProvider: BucketSizeProvider[VD, ED]) =
    new VertexMeasureConfiguration[VD, ED](bucketSizeProvider, treatAsUndirected, considerEdgeType)

  def apply[VD: ClassTag, ED: ClassTag](bucketSizeProvider: BucketSizeProvider[VD, ED]) =
    new VertexMeasureConfiguration[VD, ED](bucketSizeProvider)

}
