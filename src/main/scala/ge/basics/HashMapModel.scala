package ge.basics

import java.util.Map.Entry
import java.util.{HashMap => JHashMap}

// not consecutive vertexId
class HashMapModel() extends Serializable {
	val model: JHashMap[Int, Array[Float]] = new JHashMap[Int, Array[Float]]()
	def put(key: Int, value: Array[Float]): Unit ={
		model.put(key, value)
	}

	def get(key: Int): Array[Float] = {
		model.get(key)
	}

	def containsKey(key: Int): Boolean = {
		model.containsKey(key)
	}

	def toEntryterator(): java.util.Iterator[Entry[Int, Array[Float]]] = {
		model.entrySet().iterator()
	}

	def updateAdd(key: Int, delta: Array[Float]): Unit ={
		// we incur in-place add
		val arr = get(key)
		for(i <- 0 until(arr.length))
			arr(i) += delta(i)
	}

	def updateSub(key: Int, delta: Array[Float]): Unit ={
		// we incur in-place add
		val arr = get(key)
		for(i <- 0 until(arr.length))
			arr(i) -= delta(i)
	}

	def update(key: Int, value: Array[Float]): Unit ={
		// we change the value
		model.replace(key, value)
	}

	def merge(other: HashMapModel): HashMapModel = {
		if(this.model.size() > other.model.size()){
			this.model.putAll(other.model)
			this
		}
		else{
			other.model.putAll(this.model)
			other
		}
	}

	def deepCopy(): HashMapModel = {
		val result: HashMapModel = new HashMapModel
		val entryIter = this.toEntryterator
		while(entryIter.hasNext){
			val entry = entryIter.next()
			val key: Int = entry.getKey
			val value: Array[Float] = entry.getValue
			result.put(key, value)
		}
		result
	}

}
