package utils

/**
  * chunked dataset for efficient sampling in Spark's iterator-based scanning
  * chunk-size can be set as batch size.
  * @param chunkSize
  */
class ChunkDataset(chunkSize: Int){
	val chunkedArrays: Array[Array[Int]] = new Array[Array[Int]](chunkSize)
	var numElements = 0

	def add(data: Array[Int]): Unit = {
		chunkedArrays(numElements) = data
		numElements += 1
	}

}