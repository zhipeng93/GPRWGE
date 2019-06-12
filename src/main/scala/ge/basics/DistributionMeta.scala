package ge.basics

/**
  * class for distribution of each embeddings vector.
  * e.g., srcMeta[vertexId] = pid, source embedding of $vertexId$ is stored on partition $pid$
  * @param srcMeta
  * @param dstMeta
  */
class DistributionMeta(val srcMeta: Array[Int], val dstMeta: Array[Int]) extends Serializable{

}