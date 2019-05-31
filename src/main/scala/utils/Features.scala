package utils

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import java.util.{HashMap => JHashMap}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging

/**
  * This class provide some feature pre-processing utils for input data.
  */
object Features extends Logging{
	// splitter for data processing
	val DELIMITER = "[\\s+|,]"

	def corpusStringToIntWithoutRemapping(data: RDD[String]): RDD[Array[Int]] = {
		data.filter(f => f != null && f.length > 0)
			.map(f => f.stripLineEnd.split(DELIMITER).map(s => s.toInt))
	}

	/**
	  * map the strings to vertices and return the broadcast variable for the dict.
	  * @param data
	  * @return
	  */
	def corpusStringToChunkedIntWithRemapping(data: RDD[String],
											  batchSize: Int): (RDD[(ChunkDataset, Int)], Broadcast[Array[String]], Int) = {

		val splittedStrings: RDD[Array[String]] = data.filter(f => f != null && f.length > 0 && !f.startsWith("#"))
			.map(f => f.stripLineEnd.split(DELIMITER))

		val dict: Array[String] = splittedStrings.flatMap(f => f)
    		.distinct().collect()
		val vertexNum = dict.length
		val bcDict: Broadcast[Array[String]] = data.sparkContext.broadcast(dict)

		/**
		  * one element in this RDD: [chunk_i, size]
		  * size is not the number of sentences chunk_i,
		  * it is the number of trunks in this partition.
		  * used for efficient sampling/iterating
		  */
		val intData: RDD[(ChunkDataset, Int)] = splittedStrings.mapPartitions(iter => {
			val _dict = bcDict.value
			// build STRING2INT dict
			val string2id: JHashMap[String, Int] = new JHashMap[String, Int]((_dict.length * 1.5).toInt)
			for(i <- 0 until(_dict.length)){
				string2id.put(_dict(i), i)
			}

			// parse the dataset into chunked ints
			val chunkBuffer: ArrayBuffer[ChunkDataset] = new ArrayBuffer[ChunkDataset](100)
			var dataId = 0
			var chunkId = 0
			chunkBuffer += new ChunkDataset(batchSize)
			while(iter.hasNext){
				if((dataId + 1) % batchSize == 0){
					chunkId += 1
					chunkBuffer += new ChunkDataset(batchSize)
				}
				chunkBuffer(chunkId).add(iter.next().map(x => string2id.get(x)))
				dataId += 1
			}

			chunkBuffer.map(chunkDataSet => (chunkDataSet, chunkId + 1)).toIterator
		})
		(intData, bcDict, vertexNum)
	}

}
