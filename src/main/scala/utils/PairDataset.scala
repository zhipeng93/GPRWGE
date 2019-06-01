package utils

import scala.collection.mutable.ArrayBuffer

/**
  * use ArrayBuffer because we need to split them into pieces and merge them again.
  * @param src
  * @param dst
  */
class PairsDataset(val src: ArrayBuffer[Int], val dst: ArrayBuffer[Int]){

	def merge(other: PairsDataset): PairsDataset ={
		if(this.src.size > other.src.size) {
			this.src ++= other.src
			this.dst ++= other.dst
			this
		}
		else {
			other.src ++= this.src
			other.dst ++= this.dst
			other
		}
	}

	def toArrays(): (Array[Int], Array[Int]) = {
		(src.toArray, dst.toArray)
	}
}