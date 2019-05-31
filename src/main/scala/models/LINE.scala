package models

import org.apache.spark.rdd.RDD
import utils.{ChunkDataset, PairsDataset}

/**
  * The input of "LINE" are "word-context" pairs
  *
  * @param trainset
  * @param vertexNum
  */
class LINE(trainset: RDD[(ChunkDataset, Int)], vertexNum: Int)
  extends BaseModel(trainset, vertexNum){

  override def generatePairs(chunkDataset: ChunkDataset): PairsDataset = {
    val chunkedArrays = chunkDataset.chunkedArrays
    val src: Array[Int] = Array.ofDim(chunkDataset.numElements)
    val dst: Array[Int] = Array.ofDim(chunkDataset.numElements)
    for(i <- 0 until(src.length)){
      src(i) = chunkedArrays(i)(0)
      dst(i) = chunkedArrays(i)(1)
    }
    PairsDataset(src, dst)
  }
}