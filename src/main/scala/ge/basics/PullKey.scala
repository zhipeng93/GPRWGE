package ge.basics

import scala.collection.mutable.ArrayBuffer

/**
  * workerId needs the dstModel from other servers, listed in $keys
  * @param workerId
  * @param keys
  */
class PullKey(val workerId: Int, val keys: ArrayBuffer[Int]) extends Serializable{

	def merge(other: PullKey): PullKey = {
		if(this.keys.length < other.keys.length){
			other.keys ++= this.keys
			other
		}
		else{
			this.keys ++= other.keys
			this
		}

	}
}