package models

import org.apache.spark.rdd.RDD
import utils.{ChunkDataset, PairsDataset}
import java.util.Random

import scala.collection.mutable.ArrayBuffer

/**
  * The input of "DeepWalk" are "sentences"
  *
  * @param trainset
  * @param vertexNum
  */
class DeepWalk(trainset: RDD[(ChunkDataset, Int)], vertexNum: Int, windowSize: Int)
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

		// we adopt dynamic windowing.
		val random = new Random()
		for (i <- 0 until (chunkDataset.numElements)) {
			val walk: Array[Int] = chunkedArrays(i)
			var pos = 0
			while (pos < walk.length) {
				val dynamicWin = random.nextInt(windowSize)
				for (a <- dynamicWin until (2 * windowSize + 1 - dynamicWin)) {
					val context = pos - windowSize + a
					if (context >= 0 && context < walk.length) {
						src += walk(pos)
						dst += walk(context)
					}
				}
				pos += 1
			}
		}

		new PairsDataset(src, dst)
	}
}