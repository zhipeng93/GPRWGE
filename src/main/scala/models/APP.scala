package models

import org.apache.spark.rdd.RDD
import utils.{ChunkDataset, PairsDataset}
import java.util.Random

import scala.collection.mutable.ArrayBuffer

/**
  * The input of "App" are "sentences"
  * @param trainset
  * @param vertexNum
  */
class APP(trainset: RDD[(ChunkDataset, Int)], vertexNum: Int, stopRate: Double)
	extends BaseModel(trainset, vertexNum) {

	/**
	  * input a list of sentences, output the positive word-context pairs
	  *
	  * @param chunkDataset
	  * @return
	  */
	override def generatePairs(chunkDataset: ChunkDataset): PairsDataset = {
		val chunkedArrays = chunkDataset.chunkedArrays
		val src: ArrayBuffer[Int] = new ArrayBuffer[Int](1000)
		val dst: ArrayBuffer[Int] = new ArrayBuffer[Int](1000)

		// App follows rootedPagerank.
		// The larger the stopRate is, the shorter the random walk is.
		val random = new Random()
		for (i <- 0 until (chunkDataset.numElements)) {
			val walk: Array[Int] = chunkedArrays(i)
			val startPos = random.nextInt(walk.length)
			var endPos = startPos + 1
			while (random.nextDouble() < stopRate && endPos < walk.length){
				endPos += 1
			}
			if(endPos < walk.length){
				src += startPos
				dst += endPos
			}
		}

		PairsDataset(src.toArray, dst.toArray)
	}
}