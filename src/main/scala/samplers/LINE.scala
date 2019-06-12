package samplers

import org.apache.spark.rdd.RDD
import ge.basics.{ChunkDataset, PairsDataset}
import scala.collection.mutable.ArrayBuffer
/**
  * The input of "LINE" are "word-context" pairs
  *
  * @param trainset
  * @param vertexNum
  */
class LINE(trainset: RDD[(ChunkDataset, Int)], vertexNum: Int)
  extends BaseSampler(trainset, vertexNum){

  override def generatePairs(chunkDataset: ChunkDataset): PairsDataset = {
    val chunkedArrays = chunkDataset.chunkedArrays
    val numElements = chunkDataset.numElements
    val src: ArrayBuffer[Int] = new ArrayBuffer(numElements)
    val dst: ArrayBuffer[Int] = new ArrayBuffer(numElements)
    for(i <- 0 until(numElements)){
      src += chunkedArrays(i)(0)
      dst += chunkedArrays(i)(1)
    }
    new PairsDataset(src, dst)
  }
}